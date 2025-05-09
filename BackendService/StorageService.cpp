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
#include <iomanip>
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

SERVICE_STATUS            serviceStatus;
SERVICE_STATUS_HANDLE     serviceStatusHandle;
std::atomic<bool>         running(false);

std::mutex                serverMutex;
std::condition_variable   serverCV;
httplib::SSLServer* serverPtr = nullptr;

// Configuration struct to hold settings
struct Config {
    std::string db_dir;
    std::string db_path;
    std::string chunk_dir;
    std::string log_path;
    int http_port;
    std::string ssl_cert_path;
    std::string ssl_key_path;
    size_t payload_max_length;
};
void WriteLog(const std::string& msg, const std::string& log_path) {
    std::ofstream log(log_path, std::ios::app);
    if (log.is_open()) {
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now.time_since_epoch()) % 1000;
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&now_time_t), "%Y-%m-%d %H:%M:%S")
            << '.' << std::setfill('0') << std::setw(3) << now_ms.count();
        log << oss.str() << " - " << msg << std::endl;
    }
}


// Load configuration from config.json
Config loadConfig() {
    Config config;
    // Default values (same as original code)
    config.db_dir = "C:\\Users\\Training\\Desktop\\ProjectRoot\\BackendService";
    config.db_path = config.db_dir + "\\chunks.db";
    config.chunk_dir = config.db_dir + "\\Chunks";
    config.log_path = config.db_dir + "\\backend.log";
    config.http_port = 8081;
    config.ssl_cert_path = "C:\\Users\\Training\\Desktop\\ProjectRoot\\cert.pem";
    config.ssl_key_path = "C:\\Users\\Training\\Desktop\\ProjectRoot\\key.pem";
    config.payload_max_length = 1000 * 1024 * 1024; // Add default (100MB)

    std::ifstream config_file("config.json");
    if (!config_file.is_open()) {
        WriteLog("Warning: Failed to open config.json, using default configuration", config.log_path);
        return config;
    }

    try {
        json config_json;
        config_file >> config_json;
        config_file.close();

        auto backend = config_json.at("backend");
        config.db_dir = backend.at("db_dir").get<std::string>();
        config.db_path = config.db_dir + "\\" + backend.at("db_name").get<std::string>();
        config.chunk_dir = config.db_dir + "\\" + backend.at("chunk_dir_name").get<std::string>();
        config.log_path = config.db_dir + "\\" + backend.at("log_file_name").get<std::string>();
        config.http_port = backend.at("http_port").get<int>();
        config.ssl_cert_path = backend.at("ssl_cert_path").get<std::string>();
        config.ssl_key_path = backend.at("ssl_key_path").get<std::string>();
        if (backend.contains("payload_max_length")) { // Add this check
            config.payload_max_length = backend.at("payload_max_length").get<size_t>();
        }
    } catch (const std::exception& e) {
        WriteLog("Warning: Failed to parse config.json (" + std::string(e.what()) + "), using default configuration", config.log_path);
    }

    return config;
}


std::string stringToHex(const std::string& input) {
    std::ostringstream hex;
    for (unsigned char c : input) {
        hex << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
    }
    return hex.str();
}

std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    size_t last = str.find_last_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    return str.substr(first, (last - first + 1));
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

std::string base64Encode(const std::vector<char>& data) {
    BIO* bio, *b64;
    BUF_MEM* bufferPtr;
    b64 = BIO_new(BIO_f_base64());
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);
    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
    BIO_write(bio, data.data(), static_cast<int>(data.size()));
    BIO_flush(bio);
    BIO_get_mem_ptr(bio, &bufferPtr);
    std::string result(bufferPtr->data, bufferPtr->length);
    BIO_free_all(bio);
    return result;
}

std::string computeSHA256(const std::vector<char>& data) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), digest);
    std::ostringstream hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        hex << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
    }
    return hex.str();
}

void InitializeDatabase(const std::string& db_path, const std::string& log_path) {
    sqlite3* db = nullptr;
    if (sqlite3_open(db_path.c_str(), &db) == SQLITE_OK) {
        const char* sql_file_chunks = R"(
        CREATE TABLE IF NOT EXISTS file_chunks (
            cid TEXT,
            chunk_index INTEGER,
            hash TEXT NOT NULL,
            PRIMARY KEY (cid, chunk_index)
        );
        )";
        char* err = nullptr;
        if (sqlite3_exec(db, sql_file_chunks, nullptr, nullptr, &err) != SQLITE_OK) {
            WriteLog("DB Init Error for file_chunks: " + std::string(err), log_path);
            sqlite3_free(err);
        }

        const char* sql_chunk_references = R"(
        CREATE TABLE IF NOT EXISTS chunk_references (
            hash TEXT PRIMARY KEY,
            ref_count INTEGER NOT NULL DEFAULT 0
        );
        )";
        if (sqlite3_exec(db, sql_chunk_references, nullptr, nullptr, &err) != SQLITE_OK) {
            WriteLog("DB Init Error for chunk_references: " + std::string(err), log_path);
            sqlite3_free(err);
        }

        const char* sql_metadata = R"(
        CREATE TABLE IF NOT EXISTS file_metadata (
            cid TEXT PRIMARY KEY,
            original_filename TEXT NOT NULL
        );
        )";
        if (sqlite3_exec(db, sql_metadata, nullptr, nullptr, &err) != SQLITE_OK) {
            WriteLog("DB Init Error for file_metadata: " + std::string(err), log_path);
            sqlite3_free(err);
        } else {
            WriteLog("Database initialized (file_chunks, chunk_references, file_metadata).", log_path);
        }
        sqlite3_close(db);
    } else {
        WriteLog("Failed to open database at " + db_path, log_path);
    }
}

std::string GetOriginalFilename(const std::string& cid, sqlite3* db) {
    std::string filename;
    const char* sql = "SELECT original_filename FROM file_metadata WHERE cid = ?;";
    sqlite3_stmt* stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, cid.c_str(), -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char* fname = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            filename = fname ? fname : "";
        }
        sqlite3_finalize(stmt);
    }
    return filename;
}

std::vector<std::pair<int, std::string>> GetChunkHashesByCID(const std::string& cid, const Config& config) {
    std::vector<std::pair<int, std::string>> result;
    sqlite3* db = nullptr;
    
    if (sqlite3_open(config.db_path.c_str(), &db) != SQLITE_OK) {
        WriteLog("DB Open Error on GetChunkHashesByCID", config.log_path);
        return result;
    }

    const char* sql = "SELECT chunk_index, hash FROM file_chunks WHERE cid = ? ORDER BY chunk_index ASC;";
    sqlite3_stmt* stmt = nullptr;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, cid.c_str(), -1, SQLITE_STATIC);
        
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int chunkIndex = sqlite3_column_int(stmt, 0);
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
            if (hash) {
                result.emplace_back(chunkIndex, std::string(hash));
            }
        }
        
        sqlite3_finalize(stmt);
    } else {
        WriteLog("Prepare failed for GetChunkHashesByCID", config.log_path);
    }

    sqlite3_close(db);
    return result;
}

std::vector<char> ReadFileToVector(const std::string& filePath, const std::string& log_path) {
    std::vector<char> buffer;
    std::ifstream file(filePath, std::ios::binary);
    
    if (!file.is_open()) {
        WriteLog("Failed to open file: " + filePath, log_path);
        return buffer;
    }
    
    file.seekg(0, std::ios::end);
    size_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    buffer.resize(size);
    if (size > 0) {
        file.read(buffer.data(), size);
    }
    
    file.close();
    return buffer;
}

void RunHTTPServer(const Config& config) {
    fs::create_directories(config.chunk_dir);
    InitializeDatabase(config.db_path, config.log_path);
    WriteLog("StorageService starting HTTPS on port " + std::to_string(config.http_port), config.log_path);

    // Initialize SSLServer
    httplib::SSLServer server(config.ssl_cert_path.c_str(), config.ssl_key_path.c_str());
    server.set_payload_max_length(config.payload_max_length);// Set to 100MB to handle large file uploads
    server.set_read_timeout(60, 0); // Set read timeout to 60 seconds

    server.Post("/store", [&config](const httplib::Request& req, httplib::Response& res) {
        WriteLog("POST /store received with multipart/form-data", config.log_path);
        
        // Log all received parameters
        WriteLog("Received parameters:", config.log_path);
        for (const auto& param : req.params) {
            WriteLog("  " + param.first + ": " + param.second, config.log_path);
        }

        // Log all received files
        WriteLog("Received files:", config.log_path);
        for (const auto& file : req.files) {
            WriteLog("  " + file.first + ": " + file.second.filename + " (content_length: " + std::to_string(file.second.content.size()) + ")", config.log_path);
        }

        // Extract required fields from params or files
        std::string cid, original_filename, chunk_hashes_json;
        
        if (req.has_param("cid")) {
            cid = req.get_param_value("cid");
            WriteLog("Found cid in params: " + cid, config.log_path);
        } else if (req.files.find("cid") != req.files.end()) {
            cid = req.files.find("cid")->second.content;
            WriteLog("Found cid in files: " + cid, config.log_path);
        }

        if (req.has_param("original_filename")) {
            original_filename = req.get_param_value("original_filename");
            WriteLog("Found original_filename in params: " + original_filename, config.log_path);
        } else if (req.files.find("original_filename") != req.files.end()) {
            original_filename = req.files.find("original_filename")->second.content;
            WriteLog("Found original_filename in files: " + original_filename, config.log_path);
        }

        if (req.has_param("chunk_hashes")) {
            chunk_hashes_json = req.get_param_value("chunk_hashes");
            WriteLog("Found chunk_hashes in params: " + chunk_hashes_json, config.log_path);
        } else if (req.files.find("chunk_hashes") != req.files.end()) {
            chunk_hashes_json = req.files.find("chunk_hashes")->second.content;
            WriteLog("Found chunk_hashes in files: " + chunk_hashes_json, config.log_path);
        }

        // Validate required fields
        if (cid.empty() || original_filename.empty() || chunk_hashes_json.empty()) {
            res.status = 400;
            res.set_content("Missing required form fields", "text/plain");
            WriteLog("Missing required form fields", config.log_path);
            return;
        }

        try {
            json chunk_hashes_json_parsed = json::parse(chunk_hashes_json);
            std::vector<std::string> hashes = chunk_hashes_json_parsed.get<std::vector<std::string>>();

            WriteLog("Storing CID=" + cid + " with " + std::to_string(hashes.size()) + " chunks", config.log_path);

            sqlite3* db = nullptr;
            if (sqlite3_open(config.db_path.c_str(), &db) != SQLITE_OK) {
                res.status = 500;
                res.set_content("Database error", "text/plain");
                WriteLog("Failed to open database", config.log_path);
                return;
            }

            if (sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr) != SQLITE_OK) {
                WriteLog("Failed to begin transaction", config.log_path);
                sqlite3_close(db);
                res.status = 500;
                res.set_content("Transaction error", "text/plain");
                return;
            }

            for (size_t i = 0; i < hashes.size(); ++i) {
                std::string hash = hashes[i];
                std::string chunk_name = "chunk_" + std::to_string(i);
                
                auto file_it = req.files.find(chunk_name);
                if (file_it == req.files.end()) {
                    WriteLog("Missing chunk data for " + chunk_name, config.log_path);
                    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                    sqlite3_close(db);
                    res.status = 400;
                    res.set_content("Missing chunk data", "text/plain");
                    return;
                }
                
                std::string raw_data = file_it->second.content;

                const char* sql_select = "SELECT ref_count FROM chunk_references WHERE hash = ?;";
                sqlite3_stmt* stmt_select;
                int ref_count = 0;
                bool hash_exists = false;

                if (sqlite3_prepare_v2(db, sql_select, -1, &stmt_select, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt_select, 1, hash.c_str(), -1, SQLITE_STATIC);
                    if (sqlite3_step(stmt_select) == SQLITE_ROW) {
                        ref_count = sqlite3_column_int(stmt_select, 0);
                        hash_exists = true;
                    }
                    sqlite3_finalize(stmt_select);
                } else {
                    WriteLog("Failed to prepare SELECT for hash: " + hash, config.log_path);
                }

                if (hash_exists) {
                    const char* sql_update = "UPDATE chunk_references SET ref_count = ? WHERE hash = ?;";
                    sqlite3_stmt* stmt_update;
                    if (sqlite3_prepare_v2(db, sql_update, -1, &stmt_update, nullptr) == SQLITE_OK) {
                        sqlite3_bind_int(stmt_update, 1, ref_count + 1);
                        sqlite3_bind_text(stmt_update, 2, hash.c_str(), -1, SQLITE_STATIC);
                        if (sqlite3_step(stmt_update) != SQLITE_DONE) {
                            WriteLog("Failed to update ref_count for hash: " + hash, config.log_path);
                        }
                        sqlite3_finalize(stmt_update);
                    } else {
                        WriteLog("Failed to prepare UPDATE for hash: " + hash, config.log_path);
                    }
                } else {
                    const char* sql_insert = "INSERT INTO chunk_references (hash, ref_count) VALUES (?, 1);";
                    sqlite3_stmt* stmt_insert;
                    if (sqlite3_prepare_v2(db, sql_insert, -1, &stmt_insert, nullptr) == SQLITE_OK) {
                        sqlite3_bind_text(stmt_insert, 1, hash.c_str(), -1, SQLITE_STATIC);
                        if (sqlite3_step(stmt_insert) == SQLITE_DONE) {
                            std::string path = config.chunk_dir + "\\" + hash + ".bin";
                            std::ofstream ofs(path, std::ios::binary);
                            if (ofs) {
                                ofs.write(raw_data.data(), raw_data.size());
                                ofs.close();
                            } else {
                                WriteLog("Failed to save chunk: " + path, config.log_path);
                                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                                sqlite3_close(db);
                                res.status = 500;
                                res.set_content("Failed to save chunk", "text/plain");
                                return;
                            }
                        }
                        sqlite3_finalize(stmt_insert);
                    } else {
                        WriteLog("Failed to prepare INSERT for hash: " + hash, config.log_path);
                    }
                }

                const char* sql_file_chunks = "INSERT INTO file_chunks (cid, chunk_index, hash) VALUES (?, ?, ?);";
                sqlite3_stmt* stmt_file_chunks;
                if (sqlite3_prepare_v2(db, sql_file_chunks, -1, &stmt_file_chunks, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt_file_chunks, 1, cid.c_str(), -1, SQLITE_STATIC);
                    sqlite3_bind_int(stmt_file_chunks, 2, static_cast<int>(i) + 1);
                    sqlite3_bind_text(stmt_file_chunks, 3, hash.c_str(), -1, SQLITE_STATIC);
                    if (sqlite3_step(stmt_file_chunks) != SQLITE_DONE) {
                        WriteLog("Failed to insert into file_chunks for CID: " + cid, config.log_path);
                    }
                    sqlite3_finalize(stmt_file_chunks);
                } else {
                    WriteLog("Failed to prepare file_chunks INSERT for CID: " + cid, config.log_path);
                }
            }

            const char* sql_metadata = "INSERT INTO file_metadata (cid, original_filename) VALUES (?, ?);";
            sqlite3_stmt* stmt_metadata;
            if (sqlite3_prepare_v2(db, sql_metadata, -1, &stmt_metadata, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(stmt_metadata, 1, cid.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt_metadata, 2, original_filename.c_str(), -1, SQLITE_STATIC);
                if (sqlite3_step(stmt_metadata) != SQLITE_DONE) {
                    WriteLog("Failed to insert into file_metadata for CID: " + cid, config.log_path);
                }
                sqlite3_finalize(stmt_metadata);
            } else {
                WriteLog("Failed to prepare file_metadata INSERT", config.log_path);
            }

            if (sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr) != SQLITE_OK) {
                WriteLog("Failed to commit transaction for CID: " + cid, config.log_path);
                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db);
                res.status = 500;
                res.set_content("Transaction commit failed", "text/plain");
                return;
            }

            sqlite3_close(db);
            WriteLog("Updated reference counts for " + std::to_string(hashes.size()) + " chunks for CID: " + cid, config.log_path);
            res.status = 200;
            res.set_content("Success", "text/plain");
        } catch (std::exception& ex) {
            WriteLog(std::string("Exception in /store: ") + ex.what(), config.log_path);
            res.status = 500;
            res.set_content("Server error", "text/plain");
        }
    });

    server.Get("/file/:cid", [&config](const httplib::Request& req, httplib::Response& res) {
        std::string cid = trim(req.path_params.at("cid"));
        WriteLog("Received GET request for CID: " + cid + ", hex: " + stringToHex(cid), config.log_path);
        
        sqlite3* db = nullptr;
        if (sqlite3_open(config.db_path.c_str(), &db) != SQLITE_OK) {
            WriteLog("Failed to open database for GET CID: " + cid, config.log_path);
            res.status = 500;
            res.set_content("Database error", "text/plain");
            sqlite3_close(db);
            return;
        }

        std::vector<std::string> chunk_hashes;
        const char* sql_select = "SELECT hash FROM file_chunks WHERE cid = ? ORDER BY chunk_index;";
        sqlite3_stmt* stmt_select;
        if (sqlite3_prepare_v2(db, sql_select, -1, &stmt_select, nullptr) != SQLITE_OK) {
            WriteLog("Failed to prepare SELECT file_chunks for CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
            sqlite3_close(db);
            res.status = 500;
            res.set_content("Database query preparation failed", "text/plain");
            return;
        }

        sqlite3_bind_text(stmt_select, 1, cid.c_str(), -1, SQLITE_STATIC);
        while (sqlite3_step(stmt_select) == SQLITE_ROW) {
            const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt_select, 0));
            chunk_hashes.push_back(hash ? hash : "");
        }
        sqlite3_finalize(stmt_select);

        WriteLog("Found " + std::to_string(chunk_hashes.size()) + " chunks for CID: " + cid, config.log_path);

        if (chunk_hashes.empty()) {
            WriteLog("No chunks found for CID: " + cid, config.log_path);
            sqlite3_close(db);
            res.status = 404;
            res.set_content("File not found", "text/plain");
            return;
        }

        std::string file_data;
        for (const auto& hash : chunk_hashes) {
            std::string chunk_path = config.chunk_dir + "/" + hash + ".bin";
            std::ifstream ifs(chunk_path, std::ios::binary);
            if (!ifs) {
                WriteLog("Failed to read chunk: " + chunk_path, config.log_path);
                sqlite3_close(db);
                res.status = 500;
                res.set_content("Failed to read chunk: " + hash, "text/plain");
                return;
            }
            file_data.append(std::istreambuf_iterator<char>(ifs), {});
            ifs.close();
        }

        std::string filename = GetOriginalFilename(cid, db);
        if (filename.empty()) {
            WriteLog("No filename found for CID: " + cid + ", using default: download.bin", config.log_path);
            filename = "download.bin";
        } else {
            WriteLog("Found filename for CID: " + cid + ": " + filename, config.log_path);
        }

        sqlite3_close(db);
        WriteLog("Successfully retrieved file for CID: " + cid + " (" + std::to_string(file_data.size()) + " bytes)", config.log_path);
        res.status = 200;
        res.set_header("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        res.set_content(file_data, "application/octet-stream");
    });

    server.Post("/update", [&config](const httplib::Request& req, httplib::Response& res) {
        try {
            json payload = json::parse(req.body);
            std::string old_cid = payload.at("old_cid").get<std::string>();
            std::string new_cid = payload.at("new_cid").get<std::string>();
            auto new_hashes = payload.at("new_chunks").get<std::vector<std::string>>();
            auto new_data_b64 = payload.at("new_data").get<std::vector<std::string>>();
            std::string new_filename = payload.at("new_filename").get<std::string>();
    
            if (new_hashes.size() != new_data_b64.size()) {
                res.status = 400;
                res.set_content("Chunks/data size mismatch", "text/plain");
                WriteLog("Chunks/data size mismatch for update", config.log_path);
                return;
            }
    
            sqlite3* db = nullptr;
            if (sqlite3_open(config.db_path.c_str(), &db) != SQLITE_OK) {
                res.status = 500;
                res.set_content("Database error", "text/plain");
                WriteLog("Failed to open database for update", config.log_path);
                return;
            }
    
            sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr);
    
            std::vector<std::string> old_chunks;
            const char* sql_select_old = "SELECT hash FROM file_chunks WHERE cid = ? ORDER BY chunk_index;";
            sqlite3_stmt* stmt_select_old;
            if (sqlite3_prepare_v2(db, sql_select_old, -1, &stmt_select_old, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(stmt_select_old, 1, old_cid.c_str(), -1, SQLITE_STATIC);
                while (sqlite3_step(stmt_select_old) == SQLITE_ROW) {
                    const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt_select_old, 0));
                    old_chunks.push_back(hash);
                }
                sqlite3_finalize(stmt_select_old);
            }
    
            if (old_chunks.empty()) {
                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db);
                res.status = 404;
                res.set_content("CID not found", "text/plain");
                WriteLog("CID not found for update: " + old_cid, config.log_path);
                return;
            }
    
            if (old_chunks == new_hashes) {
                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db);
                res.status = 400;
                res.set_content("File identical - no changes required", "text/plain");
                WriteLog("File identical for update: " + old_cid, config.log_path);
                return;
            }
    
            // Decrement ref_count for old chunks and clean up if ref_count <= 0
            for (const auto& hash : old_chunks) {
                const char* sql_decrement = "UPDATE chunk_references SET ref_count = ref_count - 1 WHERE hash = ?;";
                sqlite3_stmt* stmt_decrement;
                if (sqlite3_prepare_v2(db, sql_decrement, -1, &stmt_decrement, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt_decrement, 1, hash.c_str(), -1, SQLITE_STATIC);
                    sqlite3_step(stmt_decrement);
                    sqlite3_finalize(stmt_decrement);
                }
    
                // Check if ref_count <= 0
                int ref_count = 0;
                const char* sql_select_ref = "SELECT ref_count FROM chunk_references WHERE hash = ?;";
                sqlite3_stmt* stmt_select_ref;
                if (sqlite3_prepare_v2(db, sql_select_ref, -1, &stmt_select_ref, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt_select_ref, 1, hash.c_str(), -1, SQLITE_STATIC);
                    if (sqlite3_step(stmt_select_ref) == SQLITE_ROW) {
                        ref_count = sqlite3_column_int(stmt_select_ref, 0);
                    }
                    sqlite3_finalize(stmt_select_ref);
                }
    
                // If ref_count <= 0, delete the chunk file and remove from chunk_references
                if (ref_count <= 0) {
                    std::string path = config.chunk_dir + "/" + hash + ".bin";
                    if (fs::exists(path)) {
                        fs::remove(path);
                        WriteLog("Deleted chunk file: " + path, config.log_path);
                    } else {
                        WriteLog("Chunk file not found for deletion: " + path, config.log_path);
                    }
    
                    const char* sql_delete_chunk = "DELETE FROM chunk_references WHERE hash = ?;";
                    sqlite3_stmt* stmt_delete_chunk;
                    if (sqlite3_prepare_v2(db, sql_delete_chunk, -1, &stmt_delete_chunk, nullptr) == SQLITE_OK) {
                        sqlite3_bind_text(stmt_delete_chunk, 1, hash.c_str(), -1, SQLITE_STATIC);
                        sqlite3_step(stmt_delete_chunk);
                        sqlite3_finalize(stmt_delete_chunk);
                    }
                }
            }
    
            for (size_t i = 0; i < new_hashes.size(); ++i) {
                std::string hash = new_hashes[i];
                std::string data_b64 = new_data_b64[i];
                auto raw_data = base64_decode(data_b64);
    
                int ref_count = 0;
                const char* sql_select_ref = "SELECT ref_count FROM chunk_references WHERE hash = ?;";
                sqlite3_stmt* stmt_select_ref;
                if (sqlite3_prepare_v2(db, sql_select_ref, -1, &stmt_select_ref, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt_select_ref, 1, hash.c_str(), -1, SQLITE_STATIC);
                    if (sqlite3_step(stmt_select_ref) == SQLITE_ROW) {
                        ref_count = sqlite3_column_int(stmt_select_ref, 0);
                    }
                    sqlite3_finalize(stmt_select_ref);
                }
    
                if (ref_count > 0) {
                    const char* sql_increment = "UPDATE chunk_references SET ref_count = ref_count + 1 WHERE hash = ?;";
                    sqlite3_stmt* stmt_increment;
                    if (sqlite3_prepare_v2(db, sql_increment, -1, &stmt_increment, nullptr) == SQLITE_OK) {
                        sqlite3_bind_text(stmt_increment, 1, hash.c_str(), -1, SQLITE_STATIC);
                        sqlite3_step(stmt_increment);
                        sqlite3_finalize(stmt_increment);
                    }
                } else {
                    const char* sql_insert = "INSERT INTO chunk_references (hash, ref_count) VALUES (?, 1);";
                    sqlite3_stmt* stmt_insert;
                    if (sqlite3_prepare_v2(db, sql_insert, -1, &stmt_insert, nullptr) == SQLITE_OK) {
                        sqlite3_bind_text(stmt_insert, 1, hash.c_str(), -1, SQLITE_STATIC);
                        sqlite3_step(stmt_insert);
                        sqlite3_finalize(stmt_insert);
                    }
                    std::string path = config.chunk_dir + "/" + hash + ".bin";
                    std::ofstream ofs(path, std::ios::binary);
                    if (ofs) {
                        ofs.write(raw_data.data(), raw_data.size());
                        ofs.close();
                    }
                }
            }
    
            for (size_t i = 0; i < new_hashes.size(); ++i) {
                const char* sql_insert_chunk = "INSERT INTO file_chunks (cid, chunk_index, hash) VALUES (?, ?, ?);";
                sqlite3_stmt* stmt_insert_chunk;
                if (sqlite3_prepare_v2(db, sql_insert_chunk, -1, &stmt_insert_chunk, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt_insert_chunk, 1, new_cid.c_str(), -1, SQLITE_STATIC);
                    sqlite3_bind_int(stmt_insert_chunk, 2, static_cast<int>(i) + 1);
                    sqlite3_bind_text(stmt_insert_chunk, 3, new_hashes[i].c_str(), -1, SQLITE_STATIC);
                    sqlite3_step(stmt_insert_chunk);
                    sqlite3_finalize(stmt_insert_chunk);
                }
            }
    
            const char* sql_insert_meta = "INSERT INTO file_metadata (cid, original_filename) VALUES (?, ?);";
            sqlite3_stmt* stmt_insert_meta;
            if (sqlite3_prepare_v2(db, sql_insert_meta, -1, &stmt_insert_meta, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(stmt_insert_meta, 1, new_cid.c_str(), -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt_insert_meta, 2, new_filename.c_str(), -1, SQLITE_STATIC);
                sqlite3_step(stmt_insert_meta);
                sqlite3_finalize(stmt_insert_meta);
            }
    
            const char* sql_delete_chunks = "DELETE FROM file_chunks WHERE cid = ?;";
            sqlite3_stmt* stmt_delete_chunks;
            if (sqlite3_prepare_v2(db, sql_delete_chunks, -1, &stmt_delete_chunks, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(stmt_delete_chunks, 1, old_cid.c_str(), -1, SQLITE_STATIC);
                sqlite3_step(stmt_delete_chunks);
                sqlite3_finalize(stmt_delete_chunks);
            }
    
            const char* sql_delete_meta = "DELETE FROM file_metadata WHERE cid = ?;";
            sqlite3_stmt* stmt_delete_meta;
            if (sqlite3_prepare_v2(db, sql_delete_meta, -1, &stmt_delete_meta, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(stmt_delete_meta, 1, old_cid.c_str(), -1, SQLITE_STATIC);
                sqlite3_step(stmt_delete_meta);
                sqlite3_finalize(stmt_delete_meta);
            }
    
            sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
            sqlite3_close(db);
    
            res.status = 200;
            res.set_content(new_cid, "text/plain");
            WriteLog("Successfully updated CID: " + old_cid + " to " + new_cid, config.log_path);
        } catch (const std::exception& e) {
            WriteLog("Update error: " + std::string(e.what()), config.log_path);
            res.status = 500;
            res.set_content("Server error", "text/plain");
        }
    });

    server.Get("/debug/cid/:cid", [&config](const httplib::Request& req, httplib::Response& res) {
        std::string cid = trim(req.path_params.at("cid"));
        WriteLog("Received DEBUG request for CID: " + cid + ", hex: " + stringToHex(cid), config.log_path);

        sqlite3* db = nullptr;
        if (sqlite3_open(config.db_path.c_str(), &db) != SQLITE_OK) {
            WriteLog("Failed to open database for DEBUG CID: " + cid, config.log_path);
            res.status = 500;
            res.set_content("Database error", "text/plain");
            return;
        }

        json result;
        result["cid"] = cid;

        // Check file_chunks
        std::vector<std::string> chunks;
        const char* sql_chunks = "SELECT hash FROM file_chunks WHERE cid = ? ORDER BY chunk_index;";
        sqlite3_stmt* stmt_chunks;
        if (sqlite3_prepare_v2(db, sql_chunks, -1, &stmt_chunks, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt_chunks, 1, cid.c_str(), -1, SQLITE_STATIC);
            while (sqlite3_step(stmt_chunks) == SQLITE_ROW) {
                const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt_chunks, 0));
                chunks.push_back(hash ? hash : "");
            }
            sqlite3_finalize(stmt_chunks);
        } else {
            WriteLog("Failed to prepare SELECT file_chunks for DEBUG CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
        }
        result["file_chunks_found"] = !chunks.empty();
        result["chunk_hashes"] = chunks;
        WriteLog("DEBUG CID: " + cid + ", file_chunks found: " + std::to_string(chunks.size()), config.log_path);

        // Check file_metadata
        std::string filename = GetOriginalFilename(cid, db);
        result["file_metadata_found"] = !filename.empty();
        result["original_filename"] = filename;
        WriteLog("DEBUG CID: " + cid + ", file_metadata found: " + (filename.empty() ? "false" : "true"), config.log_path);

        sqlite3_close(db);
        res.status = 200;
        res.set_content(result.dump(), "application/json");
    });
    
    server.Delete("/file/:cid", [&config](const httplib::Request& req, httplib::Response& res) {
        std::string cid = trim(req.path_params.at("cid"));
        WriteLog("Received DELETE request for CID: " + cid + ", hex: " + stringToHex(cid), config.log_path);
        
        sqlite3* db = nullptr;
        if (sqlite3_open(config.db_path.c_str(), &db) != SQLITE_OK) {
            WriteLog("Failed to open database for DELETE CID: " + cid, config.log_path);
            res.status = 500;
            res.set_content("Database error", "text/plain");
            return;
        }

        if (sqlite3_exec(db, "BEGIN;", nullptr, nullptr, nullptr) != SQLITE_OK) {
            WriteLog("Failed to begin transaction for DELETE CID: " + cid, config.log_path);
            sqlite3_close(db);
            res.status = 500;
            res.set_content("Transaction begin failed", "text/plain");
            return;
        }

        // Check file_metadata for debugging
        std::string filename = GetOriginalFilename(cid, db);
        WriteLog("DELETE CID: " + cid + ", file_metadata found: " + (filename.empty() ? "false" : "true") + ", filename: " + filename, config.log_path);

        std::vector<std::string> chunks;
        const char* sql_select_chunks = "SELECT hash FROM file_chunks WHERE cid = ?;";
        sqlite3_stmt* stmt_select_chunks;
        if (sqlite3_prepare_v2(db, sql_select_chunks, -1, &stmt_select_chunks, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt_select_chunks, 1, cid.c_str(), -1, SQLITE_STATIC);
            while (sqlite3_step(stmt_select_chunks) == SQLITE_ROW) {
                const char* hash = reinterpret_cast<const char*>(sqlite3_column_text(stmt_select_chunks, 0));
                chunks.push_back(hash ? hash : "");
            }
            sqlite3_finalize(stmt_select_chunks);
        } else {
            WriteLog("Failed to prepare SELECT chunks for CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
            sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
            sqlite3_close(db);
            res.status = 500;
            res.set_content("Database query preparation failed", "text/plain");
            return;
        }
        
        if (chunks.empty()) {
            WriteLog("No chunks found for CID: " + cid, config.log_path);
            sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
            sqlite3_close(db);
            res.status = 404;
            res.set_content("File not found", "text/plain");
            return;
        }
        WriteLog("Found " + std::to_string(chunks.size()) + " chunks for CID: " + cid, config.log_path);

        const char* sql_delete_chunks = "DELETE FROM file_chunks WHERE cid = ?;";
        sqlite3_stmt* stmt_delete_chunks;
        if (sqlite3_prepare_v2(db, sql_delete_chunks, -1, &stmt_delete_chunks, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt_delete_chunks, 1, cid.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt_delete_chunks) != SQLITE_DONE) {
                WriteLog("Failed to execute DELETE file_chunks for CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
                sqlite3_finalize(stmt_delete_chunks);
                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db);
                res.status = 500;
                res.set_content("Failed to delete chunks", "text/plain");
                return;
            }
            sqlite3_finalize(stmt_delete_chunks);
            WriteLog("Deleted file_chunks entries for CID: " + cid, config.log_path);
        } else {
            WriteLog("Failed to prepare DELETE file_chunks for CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
            sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
            sqlite3_close(db);
            res.status = 500;
            res.set_content("Failed to prepare chunk deletion", "text/plain");
            return;
        }

        const char* sql_delete_meta = "DELETE FROM file_metadata WHERE cid = ?;";
        sqlite3_stmt* stmt_delete_meta;
        if (sqlite3_prepare_v2(db, sql_delete_meta, -1, &stmt_delete_meta, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt_delete_meta, 1, cid.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt_delete_meta) != SQLITE_DONE) {
                WriteLog("Failed to execute DELETE file_metadata for CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
                sqlite3_finalize(stmt_delete_meta);
                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db);
                res.status = 500;
                res.set_content("Failed to delete metadata", "text/plain");
                return;
            }
            sqlite3_finalize(stmt_delete_meta);
            WriteLog("Deleted file_metadata entry for CID: " + cid, config.log_path);
        } else {
            WriteLog("Failed to prepare DELETE file_metadata for CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
            sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
            sqlite3_close(db);
            res.status = 500;
            res.set_content("Failed to prepare metadata deletion", "text/plain");
            return;
        }

        for (const auto& hash : chunks) {
            const char* sql_decrement = "UPDATE chunk_references SET ref_count = ref_count - 1 WHERE hash = ?;";
            sqlite3_stmt* stmt_decrement;
            if (sqlite3_prepare_v2(db, sql_decrement, -1, &stmt_decrement, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(stmt_decrement, 1, hash.c_str(), -1, SQLITE_STATIC);
                if (sqlite3_step(stmt_decrement) != SQLITE_DONE) {
                    WriteLog("Failed to execute UPDATE ref_count for hash: " + hash + ", error: " + sqlite3_errmsg(db), config.log_path);
                    sqlite3_finalize(stmt_decrement);
                    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                    sqlite3_close(db);
                    res.status = 500;
                    res.set_content("Failed to update ref_count", "text/plain");
                    return;
                }
                sqlite3_finalize(stmt_decrement);
                WriteLog("Decremented ref_count for hash: " + hash, config.log_path);
            } else {
                WriteLog("Failed to prepare UPDATE ref_count for hash: " + hash + ", error: " + sqlite3_errmsg(db), config.log_path);
                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db);
                res.status = 500;
                res.set_content("Failed to prepare ref_count update", "text/plain");
                return;
            }

            int ref_count = 0;
            const char* sql_select_ref = "SELECT ref_count FROM chunk_references WHERE hash = ?;";
            sqlite3_stmt* stmt_select_ref;
            if (sqlite3_prepare_v2(db, sql_select_ref, -1, &stmt_select_ref, nullptr) == SQLITE_OK) {
                sqlite3_bind_text(stmt_select_ref, 1, hash.c_str(), -1, SQLITE_STATIC);
                if (sqlite3_step(stmt_select_ref) == SQLITE_ROW) {
                    ref_count = sqlite3_column_int(stmt_select_ref, 0);
                } else {
                    WriteLog("No ref_count found for hash: " + hash, config.log_path);
                }
                sqlite3_finalize(stmt_select_ref);
            } else {
                WriteLog("Failed to prepare SELECT ref_count for hash: " + hash + ", error: " + sqlite3_errmsg(db), config.log_path);
                sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                sqlite3_close(db);
                res.status = 500;
                res.set_content("Failed to prepare ref_count query", "text/plain");
                return;
            }

            if (ref_count <= 0) {
                std::string path = config.chunk_dir + "/" + hash + ".bin";
                try {
                    if (fs::exists(path)) {
                        fs::remove(path);
                        WriteLog("Removed chunk file: " + path, config.log_path);
                    } else {
                        WriteLog("Chunk file not found for deletion: " + path, config.log_path);
                    }
                } catch (const fs::filesystem_error& e) {
                    WriteLog("Failed to remove chunk file: " + path + ", error: " + e.what(), config.log_path);
                    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                    sqlite3_close(db);
                    res.status = 500;
                    res.set_content("Failed to delete chunk file: " + hash, "text/plain");
                    return;
                }

                const char* sql_delete_chunk = "DELETE FROM chunk_references WHERE hash = ?;";
                sqlite3_stmt* stmt_delete_chunk;
                if (sqlite3_prepare_v2(db, sql_delete_chunk, -1, &stmt_delete_chunk, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt_delete_chunk, 1, hash.c_str(), -1, SQLITE_STATIC);
                    if (sqlite3_step(stmt_delete_chunk) != SQLITE_DONE) {
                        WriteLog("Failed to execute DELETE chunk_references for hash: " + hash + ", error: " + sqlite3_errmsg(db), config.log_path);
                        sqlite3_finalize(stmt_delete_chunk);
                        sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                        sqlite3_close(db);
                        res.status = 500;
                        res.set_content("Failed to delete chunk reference", "text/plain");
                        return;
                    }
                    sqlite3_finalize(stmt_delete_chunk);
                    WriteLog("Deleted chunk_references entry for hash: " + hash, config.log_path);
                } else {
                    WriteLog("Failed to prepare DELETE chunk_references for hash: " + hash + ", error: " + sqlite3_errmsg(db), config.log_path);
                    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
                    sqlite3_close(db);
                    res.status = 500;
                    res.set_content("Failed to prepare chunk reference deletion", "text/plain");
                    return;
                }
            }
        }

        if (sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr) != SQLITE_OK) {
            WriteLog("Failed to commit transaction for DELETE CID: " + cid + ", error: " + sqlite3_errmsg(db), config.log_path);
            sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
            sqlite3_close(db);
            res.status = 500;
            res.set_content("Transaction commit failed", "text/plain");
            return;
        }
        WriteLog("Committed transaction for DELETE CID: " + cid, config.log_path);

        sqlite3_close(db);
        WriteLog("Successfully deleted CID: " + cid, config.log_path);
        res.status = 200;
        res.set_content("Deleted", "text/plain");
    });

    serverPtr = &server;
    {
        std::lock_guard<std::mutex> lock(serverMutex);
        running = true;
    }
    serverCV.notify_all();

    if (!server.listen("0.0.0.0", config.http_port)) {
        WriteLog("Failed to start HTTP server on port " + std::to_string(config.http_port), config.log_path);
        running = false;
    }
}

void WINAPI ServiceControlHandler(DWORD controlCode) {
    switch (controlCode) {
    case SERVICE_CONTROL_STOP:
        serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        if (serverPtr) {
            serverPtr->stop();
        }
        running = false;
        serverCV.notify_all();
        serviceStatus.dwCurrentState = SERVICE_STOPPED;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        break;
    default:
        break;
    }
}

void WINAPI ServiceMain(DWORD argc, LPTSTR* argv) {
    serviceStatusHandle = RegisterServiceCtrlHandler("StorageService", ServiceControlHandler);
    if (!serviceStatusHandle) return;

    serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    serviceStatus.dwCurrentState = SERVICE_START_PENDING;
    serviceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    serviceStatus.dwWin32ExitCode = 0;
    serviceStatus.dwServiceSpecificExitCode = 0;
    serviceStatus.dwCheckPoint = 0;
    serviceStatus.dwWaitHint = 2000;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    try {
        Config config = loadConfig();
        std::thread serverThread(RunHTTPServer, config);
        serverThread.detach();
    } catch (const std::exception& e) {
        WriteLog("Failed to load config or start server: " + std::string(e.what()), "backend.log");
        serviceStatus.dwCurrentState = SERVICE_STOPPED;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        return;
    }

    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);

    std::unique_lock<std::mutex> lock(serverMutex);
    serverCV.wait(lock, [] { return !running; });
}

int main() {
    SERVICE_TABLE_ENTRY serviceTable[] = {
        { const_cast<LPSTR>("StorageService"), ServiceMain },
        { nullptr, nullptr }
    };
    if (!StartServiceCtrlDispatcher(serviceTable)) {
        WriteLog("Failed to start service dispatcher: " + std::to_string(GetLastError()), "backend.log");
        return 1;
    }
    return 0;
}