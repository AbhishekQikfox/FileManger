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
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>
#include <openssl/sha.h>

#include "httplib.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

const std::string DB_DIR   = "C:\\Users\\Training\\Desktop\\ProjectRoot\\BackendService";
const std::string DB_PATH  = DB_DIR + "\\chunks.db";
const std::string CHUNK_DIR= DB_DIR + "\\Chunks";
const int HTTP_PORT        = 8081;

SERVICE_STATUS            serviceStatus;
SERVICE_STATUS_HANDLE     serviceStatusHandle;
std::atomic<bool>         running(false);

std::mutex                serverMutex;
std::condition_variable   serverCV;
httplib::Server*          serverPtr = nullptr;

void WriteLog(const std::string& msg) {
    std::ofstream log(DB_DIR + "\\backend.log", std::ios::app);
    if (log.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt)-1] = 0;
        log << dt << " - " << msg << std::endl;
    }
}

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

std::string computeContentHash(const std::vector<std::string>& chunksB64) {
    std::string combined;
    for (const auto& chunk : chunksB64) {
        auto raw = base64_decode(chunk);
        combined.insert(combined.end(), raw.begin(), raw.end());
    }
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(combined.data()), combined.size(), digest);
    std::ostringstream hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        hex << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
    }
    return hex.str();
}

void InitializeDatabase() {
    fs::create_directories(DB_DIR);
    sqlite3* db = nullptr;
    if (sqlite3_open(DB_PATH.c_str(), &db) == SQLITE_OK) {
        const char* sql_chunks = R"(
        CREATE TABLE IF NOT EXISTS chunks_data (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cid          TEXT    NOT NULL,
            chunk_index  INTEGER NOT NULL,
            chunk_path   TEXT    NOT NULL
        );
        )";
        char* err = nullptr;
        if (sqlite3_exec(db, sql_chunks, nullptr, nullptr, &err) != SQLITE_OK) {
            WriteLog("DB Init Error for chunks_data: " + std::string(err));
            sqlite3_free(err);
        }

        const char* sql_metadata = R"(
        CREATE TABLE IF NOT EXISTS file_metadata (
            cid               TEXT    PRIMARY KEY,
            original_filename TEXT    NOT NULL
        );
        )";
        if (sqlite3_exec(db, sql_metadata, nullptr, nullptr, &err) != SQLITE_OK) {
            WriteLog("DB Init Error for file_metadata: " + std::string(err));
            sqlite3_free(err);
        }

        const char* sql_references = R"(
        CREATE TABLE IF NOT EXISTS content_references (
            content_hash TEXT    PRIMARY KEY,
            ref_count    INTEGER NOT NULL DEFAULT 1
        );
        )";
        if (sqlite3_exec(db, sql_references, nullptr, nullptr, &err) != SQLITE_OK) {
            WriteLog("DB Init Error for content_references: " + std::string(err));
            sqlite3_free(err);
        } else {
            WriteLog("Database initialized (chunks_data, file_metadata, content_references).");
        }
        sqlite3_close(db);
    } else {
        WriteLog("Failed to open database at " + DB_PATH);
    }
}

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

bool InsertFileMetadata(const std::string& cid, const std::string& original_filename) {
    sqlite3* db = nullptr;
    if (sqlite3_open(DB_PATH.c_str(), &db) != SQLITE_OK) {
        WriteLog("DB Open Error on InsertFileMetadata");
        return false;
    }

    const char* sql = "INSERT INTO file_metadata (cid, original_filename) VALUES (?, ?);";
    sqlite3_stmt* stmt = nullptr;
    bool ok = false;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, cid.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, original_filename.c_str(), -1, SQLITE_STATIC);

        if (sqlite3_step(stmt) == SQLITE_DONE) {
            WriteLog("Inserted metadata for CID=" + cid);
            ok = true;
        } else {
            WriteLog("Insert failed for metadata CID=" + cid);
        }
        sqlite3_finalize(stmt);
    } else {
        WriteLog("Prepare failed for InsertFileMetadata");
    }

    sqlite3_close(db);
    return ok;
}

bool UpdateContentReference(const std::string& content_hash) {
    sqlite3* db = nullptr;
    if (sqlite3_open(DB_PATH.c_str(), &db) != SQLITE_OK) {
        WriteLog("DB Open Error on UpdateContentReference");
        return false;
    }

    const char* sql_select = "SELECT ref_count FROM content_references WHERE content_hash = ?;";
    sqlite3_stmt* stmt_select = nullptr;
    int ref_count = 0;
    bool exists = false;

    if (sqlite3_prepare_v2(db, sql_select, -1, &stmt_select, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt_select, 1, content_hash.c_str(), -1, SQLITE_STATIC);
        if (sqlite3_step(stmt_select) == SQLITE_ROW) {
            ref_count = sqlite3_column_int(stmt_select, 0);
            exists = true;
        }
        sqlite3_finalize(stmt_select);
    }

    bool ok = false;
    if (exists) {
        const char* sql_update = "UPDATE content_references SET ref_count = ? WHERE content_hash = ?;";
        sqlite3_stmt* stmt_update = nullptr;
        if (sqlite3_prepare_v2(db, sql_update, -1, &stmt_update, nullptr) == SQLITE_OK) {
            sqlite3_bind_int(stmt_update, 1, ref_count + 1);
            sqlite3_bind_text(stmt_update, 2, content_hash.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt_update) == SQLITE_DONE) {
                WriteLog("Incremented ref_count to " + std::to_string(ref_count + 1) + " for hash=" + content_hash);
                ok = true;
            }
            sqlite3_finalize(stmt_update);
        }
    } else {
        const char* sql_insert = "INSERT INTO content_references (content_hash, ref_count) VALUES (?, 1);";
        sqlite3_stmt* stmt_insert = nullptr;
        if (sqlite3_prepare_v2(db, sql_insert, -1, &stmt_insert, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt_insert, 1, content_hash.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt_insert) == SQLITE_DONE) {
                WriteLog("Inserted new content hash=" + content_hash + " with ref_count=1");
                ok = true;
            }
            sqlite3_finalize(stmt_insert);
        }
    }

    sqlite3_close(db);
    return ok;
}

std::string GetOriginalFilename(const std::string& cid) {
    sqlite3* db = nullptr;
    if (sqlite3_open(DB_PATH.c_str(), &db) != SQLITE_OK) {
        WriteLog("DB Open Error on GetOriginalFilename");
        return "";
    }

    const char* sql = "SELECT original_filename FROM file_metadata WHERE cid = ?;";
    sqlite3_stmt* stmt = nullptr;
    std::string filename;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, cid.c_str(), -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* fn = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (fn) filename = fn;
        }
        sqlite3_finalize(stmt);
    } else {
        WriteLog("Prepare failed for GetOriginalFilename");
    }

    sqlite3_close(db);
    return filename;
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

std::vector<char> ReadFileToVector(const std::string& filePath) {
    std::vector<char> buffer;
    std::ifstream file(filePath, std::ios::binary);
    
    if (!file.is_open()) {
        WriteLog("Failed to open file: " + filePath);
        return buffer;
    }
    
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    buffer.resize(size);
    if (size > 0) {
        file.read(buffer.data(), size);
    }
    
    return buffer;
}

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
            std::string original_filename = payload.at("original_filename").get<std::string>();

            if (hashes.size() != chunksB64.size()) {
                res.status = 400;
                res.set_content("chunks/data size mismatch", "text/plain");
                return;
            }

            WriteLog("Storing CID=" + cid + " with " + std::to_string(hashes.size()) + " chunks");

            std::string content_hash = computeContentHash(chunksB64);
            UpdateContentReference(content_hash);

            for (size_t i = 0; i < hashes.size(); ++i) {
                int    index = static_cast<int>(i) + 1;
                auto   raw   = base64_decode(chunksB64[i]);
                if (raw.empty()) {
                    WriteLog("Decode failed for chunk " + std::to_string(i));
                    continue;
                }
                std::string path = CHUNK_DIR + "\\" + cid + "_chunk" + std::to_string(index) + ".bin";
                std::ofstream ofs(path, std::ios::binary);
                ofs.write(raw.data(), raw.size());
                ofs.close();
                InsertChunkMetadata(cid, index, path);
            }

            InsertFileMetadata(cid, original_filename);

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
        
        auto chunks = GetChunkPathsByCID(cid);
        
        if (chunks.empty()) {
            res.status = 404;
            res.set_content("File not found", "text/plain");
            WriteLog("No chunks found for CID: " + cid);
            return;
        }
        
        WriteLog("Found " + std::to_string(chunks.size()) + " chunks for CID: " + cid);
        
        std::string original_filename = GetOriginalFilename(cid);
        if (original_filename.empty()) {
            WriteLog("No metadata found for CID: " + cid);
            original_filename = "file-" + cid;
        }

        json response;
        response["cid"] = cid;
        response["original_filename"] = original_filename;
        response["chunks"] = json::array();
        
        for (const auto& [index, path] : chunks) {
            auto chunkData = ReadFileToVector(path);
            if (chunkData.empty()) {
                WriteLog("Failed to read chunk file: " + path);
                continue;
            }
            
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
        running = true;
        RunHTTPServer();
    }
    return 0;
}