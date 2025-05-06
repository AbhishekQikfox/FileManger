#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <thread>
#include <atomic>
// Add OpenSSL headers for base64 decoding
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>
#include "httplib.h" // Add httplib for HTTP server

namespace fs = std::filesystem;
using json = nlohmann::json;

const std::string DB_DIR = "C:\\Users\\Training\\Desktop\\ProjectRoot\\BackendService";
const std::string DB_PATH = DB_DIR + "\\chunks.db";
const std::string CHUNK_DIR = "C:\\Users\\Training\\Desktop\\ProjectRoot\\BackendService\\Chunks";
const int HTTP_PORT = 8081; // Port for the HTTP server

SERVICE_STATUS serviceStatus;
SERVICE_STATUS_HANDLE serviceStatusHandle;
std::atomic<bool> running(false);

// Server mutex and condition variable
std::mutex serverMutex;
std::condition_variable serverCV;
httplib::Server* serverPtr = nullptr;

// Proper base64 decoder using OpenSSL
std::vector<char> base64_decode(const std::string& input) {
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); // Same flag as in encoding
    
    BIO* bmem = BIO_new_mem_buf(input.c_str(), static_cast<int>(input.length()));
    bmem = BIO_push(b64, bmem);
    
    std::vector<char> buffer(input.size()); // Should be large enough for the decoded data
    int decodedSize = BIO_read(bmem, buffer.data(), static_cast<int>(buffer.size()));
    
    BIO_free_all(bmem);
    
    if (decodedSize > 0) {
        buffer.resize(decodedSize); // Adjust to actual size
        return buffer;
    }
    
    return std::vector<char>(); // Return empty vector on error
}

void WriteLog(const std::string& msg) {
    std::ofstream log("C:\\Users\\Training\\Desktop\\ProjectRoot\\BackendService\\backend.log", std::ios::app);
    if (log.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt) - 1] = 0; // Remove newline
        log << dt << " - " << msg << std::endl;
        log.flush();
    }
}

// Database
void InitializeDatabase() {
    sqlite3* db;
    if (sqlite3_open(DB_PATH.c_str(), &db) == SQLITE_OK) {
        const char* sql = R"(
        CREATE TABLE IF NOT EXISTS chunk_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cid TEXT NOT NULL,
            chunk_hash TEXT NOT NULL,
            chunk_path TEXT NOT NULL
        );
        )";
        char* errMsg = nullptr;
        if (sqlite3_exec(db, sql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
            WriteLog("DB Error: " + std::string(errMsg));
            sqlite3_free(errMsg);
        }
        sqlite3_close(db);
    } else {
        WriteLog("Failed to open database.");
    }
}

bool InsertChunkMetadata(const std::string& cid, const std::string& hash, const std::string& path) {
    sqlite3* db;
    if (sqlite3_open(DB_PATH.c_str(), &db) != SQLITE_OK) {
        WriteLog("Failed to open database for insertion.");
        return false;
    }

    std::string sql = "INSERT INTO chunk_index (cid, chunk_hash, chunk_path) VALUES (?, ?, ?);";
    sqlite3_stmt* stmt;
    bool result = false;
    
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, cid.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, hash.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, path.c_str(), -1, SQLITE_STATIC);
        
        int step_result = sqlite3_step(stmt);
        if (step_result == SQLITE_DONE) {
            WriteLog("Successfully inserted metadata for chunk: " + hash);
            result = true;
        } else {
            WriteLog("Failed to insert metadata. Error code: " + std::to_string(step_result));
        }
        
        sqlite3_finalize(stmt);
    } else {
        WriteLog("Failed to prepare SQL statement");
    }
    
    sqlite3_close(db);
    return result;
}

// HTTP server implementation to replace the named pipe
void RunHTTPServer() {
    fs::create_directories(CHUNK_DIR);
    InitializeDatabase();
    WriteLog("ObjectStore service started. Database initialized.");

    httplib::Server server;

    // Handle POST requests to store chunks
    server.Post("/store", [](const httplib::Request& req, httplib::Response& res) {
        WriteLog("Received HTTP request to store chunks");
        
        if (req.get_header_value("Content-Type") != "application/json") {
            WriteLog("Invalid content type. Expected application/json");
            res.status = 400;
            res.set_content("Invalid content type", "text/plain");
            return;
        }
        
        try {
            std::string jsonStr = req.body;
            WriteLog("Received JSON data of size: " + std::to_string(jsonStr.size()));
            
            json payload = json::parse(jsonStr);
            if (!payload.contains("cid") || !payload.contains("chunks") || !payload.contains("data")) {
                WriteLog("Missing required fields in JSON payload");
                res.status = 400;
                res.set_content("Missing required fields", "text/plain");
                return;
            }

            std::string cid = payload["cid"];
            auto hashes = payload["chunks"].get<std::vector<std::string>>();
            auto chunks = payload["data"].get<std::vector<std::string>>();

            if (hashes.size() != chunks.size()) {
                WriteLog("Error: Mismatch between hash count and chunk count");
                res.status = 400;
                res.set_content("Mismatch between hash count and chunk count", "text/plain");
                return;
            }

            WriteLog("Processing CID: " + cid + " with " + std::to_string(hashes.size()) + " chunks");
            fs::create_directories(CHUNK_DIR);

            for (size_t i = 0; i < hashes.size(); ++i) {
                std::string hash = hashes[i];
                std::vector<char> chunkData = base64_decode(chunks[i]);
                if (chunkData.empty()) {
                    WriteLog("Failed to decode chunk " + std::to_string(i) + " with hash " + hash);
                    continue;
                }

                std::string chunkPath = CHUNK_DIR + "/" + hash + ".bin";
                std::ofstream out(chunkPath, std::ios::binary);
                if (!out.is_open()) {
                    WriteLog("Failed to open file for writing: " + chunkPath);
                    continue;
                }

                out.write(chunkData.data(), chunkData.size());
                out.close();

                if (!InsertChunkMetadata(cid, hash, chunkPath)) {
                    WriteLog("Failed to insert metadata for chunk: " + hash);
                }
            }

            WriteLog("Successfully handled CID: " + cid);
            res.status = 200;
            res.set_content("Success", "text/plain");
            
        } catch (const std::exception& e) {
            WriteLog("Error processing data: " + std::string(e.what()));
            res.status = 500;
            res.set_content("Internal server error: " + std::string(e.what()), "text/plain");
        }
    });
    
    // Add health check endpoint
    server.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content("OK", "text/plain");
    });

    {
        std::lock_guard<std::mutex> lock(serverMutex);
        serverPtr = &server;
        serverCV.notify_one();
    }

    WriteLog("HTTP server started on port " + std::to_string(HTTP_PORT));
    server.listen("0.0.0.0", HTTP_PORT);
    WriteLog("HTTP server stopped");
}

// Service Control Handler
void WINAPI ServiceCtrlHandler(DWORD ctrlCode) {
    switch (ctrlCode) {
    case SERVICE_CONTROL_STOP:
        WriteLog("Received stop request");
        serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
        serviceStatus.dwWaitHint = 30000; // 30 seconds wait hint
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        
        // Signal our main thread to stop
        running = false;
        
        // Stop the HTTP server if it's running
        {
            std::lock_guard<std::mutex> lock(serverMutex);
            if (serverPtr) serverPtr->stop();
        }
        break;
    }
}

// Service Main Function
void WINAPI ServiceMain(DWORD argc, LPSTR* argv) {
    serviceStatusHandle = RegisterServiceCtrlHandler("ObjectStore", ServiceCtrlHandler);
    
    // Initialize service status
    ZeroMemory(&serviceStatus, sizeof(serviceStatus));
    serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    serviceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    serviceStatus.dwCurrentState = SERVICE_START_PENDING;
    serviceStatus.dwWaitHint = 10000; // 10 seconds for startup
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    running = true;

    std::thread serverThread(RunHTTPServer);
    WriteLog("Server thread started");

    {
        std::unique_lock<std::mutex> lock(serverMutex);
        serverCV.wait(lock, [] { return serverPtr != nullptr; });
    }
    
    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    // Wait until stop is triggered
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    WriteLog("Waiting for server thread to exit");
    if (serverThread.joinable()) serverThread.join();
    WriteLog("Server thread exited");

    serviceStatus.dwCurrentState = SERVICE_STOPPED;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
    WriteLog("Service stopped");
}

// Entry point
int main() {
    SERVICE_TABLE_ENTRY serviceTable[] = {
        { (LPSTR)"ObjectStore", ServiceMain },
        { NULL, NULL }
    };

    if (!StartServiceCtrlDispatcher(serviceTable)) {
        DWORD error = GetLastError();
        // If called from command line, not as service
        if (error == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
            WriteLog("Running as standalone application (not as service)");
            running = true;
            RunHTTPServer();
            return 0;
        }
        
        WriteLog("Failed to start service control dispatcher. Error code: " + std::to_string(error));
    }

    return 0;
}