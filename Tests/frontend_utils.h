#pragma once

#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <random>
#include <openssl/sha.h>

// Convert arbitrary bytes (in a std::string) to hex
inline std::string stringToHex(const std::string& input) {
    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    for (unsigned char c : input) {
        hex << std::setw(2) << static_cast<int>(c);
    }
    return hex.str();
}

// Trim leading/trailing whitespace
inline std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, last - first + 1);
}

// Compute SHA-256 hash of a byte buffer (vector<char>) and return hex string
inline std::string computeHash(const std::vector<char>& data) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.data()),
           data.size(), digest);
    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        hex << std::setw(2) << static_cast<int>(digest[i]);
    }
    return hex.str();
}

// Generate a random 16-hex-digit string
inline std::string randomHex64() {
    static std::mt19937_64 eng{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist;
    uint64_t v = dist(eng);
    std::ostringstream ss;
    ss << std::hex << std::setw(16) << std::setfill('0') << v;
    return ss.str();
}

// Generate CID: sha256(concat all chunk hashes) + '_' + randomHex64()
inline std::string generateCID(const std::vector<std::string>& chunkHashes) {
    std::string combined;
    combined.reserve(chunkHashes.size() * SHA256_DIGEST_LENGTH * 2);
    for (auto& h : chunkHashes) combined += h;
    // sha256 of the combined string:
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(combined.data()),
           combined.size(), digest);
    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i)
        hex << std::setw(2) << static_cast<int>(digest[i]);
    // append underscore + random suffix
    hex << "_" << randomHex64();
    return hex.str();
}
