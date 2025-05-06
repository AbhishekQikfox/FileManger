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
#include <mutex>
#include <condition_variable>

// OpenSSL headers for base64 decoding
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

#include "httplib.h" // HTTP server

namespace fs = std::filesystem;
using json = nlohmann::json;

const std::string DB_DIR   = "C:\\Users\\Training\\Desktop\\ProjectRoot\\BackendService";
const std::string DB_PATH  = DB_DIR + "\\chunks.db";
const std::string CHUNK_DIR= DB_DIR + "\\Chunks";
const int HTTP_PORT        = 8081; // Port for the HTTP server

SERVICE_STATUS            serviceStatus;
SERVICE_STATUS_HANDLE     serviceStatusHandle;
std::atomic<bool>         running(false);

// For coordinating server startup/shutdown
std::mutex                serverMutex;
std::condition_variable   serverCV;
httplib::Server*          serverPtr = nullptr;

// ----------------------------------------------------------------------------
// Logging helper
// ----------------------------------------------------------------------------
void WriteLog(const std::string& msg) {
    std::ofstream log(DB_DIR + "\\backend.log", std::ios::app);
    if (log.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt)-1] = 0; // strip newline
        log << dt << " - " << msg << std::endl;
    }
}

// ----------------------------------------------------------------------------
// Base64 decode using OpenSSL
// ----------------------------------------------------------------------------
std::vector<char> base64_decode(const std::string& input) {
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    BIO* bmem = BIO_new_mem_buf(input.data(), static_cast<int>(input.size()));
    bmem = BIO_push(b64, bmem);

    std::vector<char> buffer(input.size());
    int decodedLen = BIO_read(bmem, buffer.data(), static_cast<int>(buffer.size()));
    BIO_free_all(bmem);

    if (decodedLen > 0) {
        buffer.resize(decodedLen);
        return buffer;
    }
    return {};
}

// ----------------------------------------------------------------------------
// Create or migrate database
// ----------------------------------------------------------------------------
void InitializeDatabase() {
    fs::create_directories(DB_DIR);
    sqlite3* db = nullptr;
    if (sqlite3_open(DB_PATH.c_str(), &db) == SQLITE_OK) {
        // Create table chunks_data
        const char* sql = R"(
        CREATE TABLE IF NOT EXISTS chunks_data (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cid          TEXT    NOT NULL,
            chunk_index  INTEGER NOT NULL,
            chunk_path   TEXT    NOT NULL
        );
        )";
        char* err = nullptr;
        if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            WriteLog("DB Init Error: " + std::string(err));
            sqlite3_free(err);
        } else {
            WriteLog("Database initialized (chunks_data).");
        }
        sqlite3_close(db);
    } else {
        WriteLog("Failed to open database at " + DB_PATH);
    }
}

// ----------------------------------------------------------------------------
// Insert one row of metadata: cid + chunk_index + path
// ----------------------------------------------------------------------------
bool InsertChunkMetadata(const std::string& cid, int chunkIndex, const std::string& path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(DB_PATH.c_str(), &db) != SQLITE_OK) {
        WriteLog("DB Open Error on insert");
        return false;
    }

    const char* sql = "INSERT INTO chunks_data (cid, chunk_index, chunk_path) VALUES (?, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    bool ok = false;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, cid.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_int (stmt, 2, chunkIndex);
        sqlite3_bind_text(stmt, 3, path.c_str(), -1, SQLITE_STATIC);

        if (sqlite3_step(stmt) == SQLITE_DONE) {
            WriteLog("Inserted chunk_index=" + std::to_string(chunkIndex) + " for CID=" + cid);
            ok = true;
        } else {
            WriteLog("Insert failed for chunk_index=" + std::to_string(chunkIndex));
        }
        sqlite3_finalize(stmt);
    } else {
        WriteLog("Prepare failed for InsertChunkMetadata");
    }

    sqlite3_close(db);
    return ok;
}

std::vector<std::pair<int, std::string>> GetChunkPathsByCID(const std::string& cid) {
    std::vector<std::pair<int, std::string>> result;
    sqlite3* db = nullptr;
    
    if (sqlite3_open(DB_PATH.c_str(), &db) != SQLITE_OK) {
        WriteLog("DB Open Error on GetChunkPathsByCID");
        return result;
    }

    const char* sql = "SELECT chunk_index, chunk_path FROM chunks_data WHERE cid = ? ORDER BY chunk_index ASC;";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, cid.c_str(), -1, SQLITE_STATIC);
        
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int chunkIndex = sqlite3_column_int(stmt, 0);
            const char* path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            if (path) {
                result.emplace_back(chunkIndex, std::string(path));
            }
        }
        
        sqlite3_finalize(stmt);
    } else {
        WriteLog("Prepare failed for GetChunkPathsByCID");
    }

    sqlite3_close(db);
    return result;
}

// Add this function to read a file into a vector
std::vector<char> ReadFileToVector(const std::string& filePath) {
    std::vector<char> buffer;
    std::ifstream file(filePath, std::ios::binary);
    
    if (!file.is_open()) {
        WriteLog("Failed to open file: " + filePath);
        return buffer;
    }
    
    // Get file size
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    // Read file content
    buffer.resize(size);
    if (size > 0) {
        file.read(buffer.data(), size);
    }
    
    return buffer;
}


// ----------------------------------------------------------------------------
// HTTP server: accept JSON with "cid", "chunks" (hashes), "data" (base64)
// ----------------------------------------------------------------------------
void RunHTTPServer() {
    fs::create_directories(CHUNK_DIR);
    InitializeDatabase();
    WriteLog("StorageService starting HTTP on port " + std::to_string(HTTP_PORT));

    httplib::Server server;

    server.Post("/store", [](const httplib::Request& req, httplib::Response& res) {
        WriteLog("POST /store received, Content-Length=" + std::to_string(req.body.size()));
        if (req.get_header_value("Content-Type") != "application/json") {
            res.status = 400;
            res.set_content("Expected application/json", "text/plain");
            return;
        }

        try {
            json payload   = json::parse(req.body);
            std::string cid= payload.at("cid").get<std::string>();
            auto hashes    = payload.at("chunks").get<std::vector<std::string>>();
            auto chunksB64 = payload.at("data").get<std::vector<std::string>>();

            if (hashes.size() != chunksB64.size()) {
                res.status = 400;
                res.set_content("chunks/data size mismatch", "text/plain");
                return;
            }

            WriteLog("Storing CID=" + cid + " with " + std::to_string(hashes.size()) + " chunks");

            for (size_t i = 0; i < hashes.size(); ++i) {
                int    index = static_cast<int>(i) + 1;
                auto   raw   = base64_decode(chunksB64[i]);
                if (raw.empty()) {
                    WriteLog("Decode failed for chunk " + std::to_string(i));
                    continue;
                }
                // write out file
                std::string path = CHUNK_DIR + "\\" + cid + "_chunk" + std::to_string(index) + ".bin";
                std::ofstream ofs(path, std::ios::binary);
                ofs.write(raw.data(), raw.size());
                ofs.close();
                // insert metadata
                InsertChunkMetadata(cid, index, path);
            }

            res.status = 200;
            res.set_content("Success", "text/plain");
        }
        catch (std::exception& ex) {
            WriteLog(std::string("Exception in /store: ") + ex.what());
            res.status = 500;
            res.set_content("Server error", "text/plain");
        }
    });

    server.Get("/health", [](auto&, auto& res){
        res.status = 200; res.set_content("OK","text/plain");
    });

    server.Get("/file/:cid", [](const httplib::Request& req, httplib::Response& res) {
        std::string cid = req.path_params.at("cid");
        WriteLog("GET /file/" + cid + " received");
        
        // Get all chunk paths for this CID
        auto chunks = GetChunkPathsByCID(cid);
        
        if (chunks.empty()) {
            res.status = 404;
            res.set_content("File not found", "text/plain");
            WriteLog("No chunks found for CID: " + cid);
            return;
        }
        
        WriteLog("Found " + std::to_string(chunks.size()) + " chunks for CID: " + cid);
        
        // Prepare JSON response with chunk data
        json response;
        response["cid"] = cid;
        response["chunks"] = json::array();
        
        for (const auto& [index, path] : chunks) {
            auto chunkData = ReadFileToVector(path);
            if (chunkData.empty()) {
                WriteLog("Failed to read chunk file: " + path);
                continue;
            }
            
            // Base64 encode the chunk
            BIO* b64 = BIO_new(BIO_f_base64());
            BIO* bmem = BIO_new(BIO_s_mem());
            BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
            BIO_push(b64, bmem);
    
            BIO_write(b64, chunkData.data(), static_cast<int>(chunkData.size()));
            BIO_flush(b64);
            
            BUF_MEM* bptr;
            BIO_get_mem_ptr(b64, &bptr);
            
            std::string encodedChunk(bptr->data, bptr->length);
            BIO_free_all(b64);
            
            // Add to response
            json chunkObj;
            chunkObj["index"] = index;
            chunkObj["data"] = encodedChunk;
            response["chunks"].push_back(chunkObj);
        }
        
        res.set_content(response.dump(), "application/json");
        WriteLog("Successfully sent file data for CID: " + cid);
    });

    {
        std::lock_guard<std::mutex> lk(serverMutex);
        serverPtr = &server;
        serverCV.notify_one();
    }

    server.listen("0.0.0.0", HTTP_PORT);
    WriteLog("HTTP server stopped");
}

// ----------------------------------------------------------------------------
// Windows Service plumbing
// ----------------------------------------------------------------------------
void WINAPI ServiceCtrlHandler(DWORD code) {
    if (code == SERVICE_CONTROL_STOP) {
        WriteLog("Service stopping...");
        running = false;
        SERVICE_STATUS ss = {};
        ss.dwCurrentState = SERVICE_STOP_PENDING;
        SetServiceStatus(serviceStatusHandle, &ss);
        if (serverPtr) serverPtr->stop();
    }
}

void WINAPI ServiceMain(DWORD, LPSTR*) {
    serviceStatusHandle = RegisterServiceCtrlHandler("ObjectStore", ServiceCtrlHandler);
    running = true;
    std::thread t(RunHTTPServer);

    SERVICE_STATUS ss = {};
    ss.dwServiceType    = SERVICE_WIN32_OWN_PROCESS;
    ss.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    ss.dwCurrentState   = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &ss);

    {
        std::unique_lock<std::mutex> lk(serverMutex);
        serverCV.wait(lk, []{ return serverPtr != nullptr; });
    }

    while (running) std::this_thread::sleep_for(std::chrono::seconds(1));
    t.join();

    ss.dwCurrentState = SERVICE_STOPPED;
    SetServiceStatus(serviceStatusHandle, &ss);
}

int main() {
    SERVICE_TABLE_ENTRY table[] = {
        { (LPSTR)"ObjectStore", ServiceMain },
        { nullptr, nullptr }
    };
    if (!StartServiceCtrlDispatcher(table)) {
        // run as console
        running = true;
        RunHTTPServer();
    }
    return 0;
}
