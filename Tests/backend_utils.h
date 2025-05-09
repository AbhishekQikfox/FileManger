#pragma once
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <openssl/sha.h>
#include <sstream>
#include <iomanip>

// filesystem & JSON helpers
inline std::vector<std::pair<int,std::string>> 
GetChunkHashesByCID(const std::string& cid, const std::string& dbPath) {
    std::vector<std::pair<int,std::string>> result;
    sqlite3* db = nullptr;
    if (sqlite3_open(dbPath.c_str(), &db) != SQLITE_OK) return result;

    const char* sql = "SELECT chunk_index,hash FROM file_chunks WHERE cid=? ORDER BY chunk_index";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr)==SQLITE_OK) {
        sqlite3_bind_text(stmt,1,cid.c_str(),-1,SQLITE_STATIC);
        while (sqlite3_step(stmt)==SQLITE_ROW) {
            result.emplace_back(
              sqlite3_column_int(stmt,0),
              reinterpret_cast<const char*>(sqlite3_column_text(stmt,1))
            );
        }
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return result;
}

inline std::vector<char> ReadFileToVector(const std::string& path) {
    std::vector<char> buf;
    std::ifstream f(path, std::ios::binary);
    if (!f) return buf;
    f.seekg(0,std::ios::end);
    size_t n = f.tellg();
    f.seekg(0,std::ios::beg);
    buf.resize(n);
    if (n) f.read(buf.data(),n);
    return buf;
}

inline std::string computeSHA256(const std::vector<char>& data) {
    unsigned char d[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), d);
    std::ostringstream o;
    o << std::hex << std::setfill('0');
    for (int i=0;i<SHA256_DIGEST_LENGTH;i++) o<<std::setw(2)<<(int)d[i];
    return o.str();
}

inline std::string trim(const std::string& s) {
    auto f = s.find_first_not_of(" \t\n\r");
    if (f==std::string::npos) return "";
    auto l = s.find_last_not_of(" \t\n\r");
    return s.substr(f,l-f+1);
}
