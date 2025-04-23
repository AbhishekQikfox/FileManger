#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <mutex>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <thread>
#include <future>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <openssl/sha.h>
#include <nlohmann/json.hpp>
#include "httplib.h"

namespace fs = std::filesystem;
using json = nlohmann::json;

const size_t CHUNK_SIZE = 1024 * 1024; // 1MB

class ThreadPool {
public:
    ThreadPool(unsigned int threads) : stop(false) {
        for (unsigned int i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex);
                        condition.wait(lock, [this] { return stop.load() || !tasks.empty(); });
                        if (stop.load() && tasks.empty()) return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        stop.store(true);
        condition.notify_all();
        for (std::thread &worker : workers)
            worker.join();
    }

    std::future<std::string> enqueue(std::function<std::string()> task) {
        auto packaged_task = std::make_shared<std::packaged_task<std::string()>>(task);
        std::future<std::string> result = packaged_task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (stop.load())
                throw std::runtime_error("Enqueue on stopped ThreadPool");
            tasks.push([packaged_task]() { (*packaged_task)(); });
        }
        condition.notify_one();
        return result;
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
};

class SimpleFileManager {
private:
    std::mutex chunkMutex;
    std::string chunksDir = "./chunks/";
    std::string metadataDir = "./metadata/";
    std::string refCountFile = "./chunk_refs.json";

    std::string computeHash(const std::vector<char> &data) {
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256((const unsigned char *)data.data(), data.size(), hash);
        char hexstr[65];
        for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
            sprintf(hexstr + i * 2, "%02x", hash[i]);
        hexstr[64] = 0;
        return std::string(hexstr);
    }

    json readRefCounts() {
        std::lock_guard<std::mutex> lock(chunkMutex);
        json refCounts;
        if (fs::exists(refCountFile)) {
            try {
                std::ifstream ifs(refCountFile);
                if (ifs.is_open()) {
                    ifs >> refCounts;
                    ifs.close();
                }
            } catch (const std::exception &e) {
                std::cerr << "Warning: Corrupted chunk_refs.json, resetting: " << e.what() << std::endl;
                refCounts = json::object();
            }
        }
        return refCounts.empty() ? json::object() : refCounts;
    }

    void writeRefCounts(const json &refCounts) {
        std::lock_guard<std::mutex> lock(chunkMutex);
        std::ofstream ofs(refCountFile);
        if (!ofs)
            throw std::runtime_error("Failed to open chunk_refs.json for writing");
        ofs << refCounts.dump(4);
        ofs.close();
    }

    void incrementRefCount(const std::string &hash) {
        json refCounts = readRefCounts();
        refCounts[hash] = refCounts.value(hash, 0) + 1;
        writeRefCounts(refCounts);
    }

    void saveChunk(const std::string &hash, const std::vector<char> &chunk) {
        std::string chunkPath = chunksDir + hash;
        if (!fs::exists(chunkPath)) {
            std::lock_guard<std::mutex> lock(chunkMutex);
            if (!fs::exists(chunkPath)) {
                std::ofstream ofs(chunkPath, std::ios::binary);
                if (!ofs)
                    throw std::runtime_error("Failed to open chunk file: " + chunkPath);
                ofs.write(chunk.data(), chunk.size());
                if (!ofs)
                    throw std::runtime_error("Failed to write chunk: " + chunkPath);
                ofs.close();
            }
        }
    }

    std::string getTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        char timestr[21];
        strftime(timestr, sizeof(timestr), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&time));
        return std::string(timestr);
    }

public:
    SimpleFileManager() {
        fs::create_directories(chunksDir);
        fs::create_directories(metadataDir);
    }

    void storeFileData(const std::string &filename, const std::vector<char> &data) {
        std::string metaPath = metadataDir + filename + ".json";
        if (fs::exists(metaPath))
            throw std::runtime_error("File already exists: " + filename);

        std::vector<std::string> chunkHashes;
        size_t totalSize = 0;
        ThreadPool pool(std::thread::hardware_concurrency());
        std::vector<std::future<std::string>> futures;

        size_t offset = 0;
        while (offset < data.size()) {
            size_t bytesRead = std::min(CHUNK_SIZE, data.size() - offset);
            std::vector<char> chunk(data.begin() + offset, data.begin() + offset + bytesRead);
            totalSize += bytesRead;
            futures.push_back(pool.enqueue([this, chunk]() {
                std::string hash = computeHash(chunk);
                saveChunk(hash, chunk);
                incrementRefCount(hash);
                return hash;
            }));
            offset += bytesRead;
        }

        for (auto &f : futures) {
            chunkHashes.push_back(f.get());
        }

        json metadata;
        metadata["filename"] = filename;
        metadata["size"] = totalSize;
        metadata["created_at"] = getTimestamp();
        metadata["chunks"] = chunkHashes;

        std::ofstream ofs(metaPath);
        if (!ofs)
            throw std::runtime_error("Failed to write metadata: " + metaPath);
        ofs << metadata.dump(4);
        ofs.close();
        std::cout << "File " << filename << " stored successfully" << std::endl;
    }

    void storeFile(const std::string &inputFilePath) {
        std::ifstream ifs(inputFilePath, std::ios::binary);
        if (!ifs)
            throw std::runtime_error("Failed to open input file: " + inputFilePath);
        std::vector<char> data((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        ifs.close();
        std::string filename = fs::path(inputFilePath).filename().string();
        storeFileData(filename, data);
    }

    std::vector<std::string> listFiles() {
        std::vector<std::string> files;
        for (const auto &entry : fs::directory_iterator(metadataDir)) {
            if (entry.path().extension() == ".json") {
                files.push_back(entry.path().stem().string());
            }
        }
        return files;
    }

    std::vector<char> retrieveFile(const std::string &filename) {
        std::string metaPath = metadataDir + filename + ".json";
        if (!fs::exists(metaPath))
            throw std::runtime_error("File not found: " + filename);

        json metadata;
        {
            std::ifstream ifs(metaPath);
            if (!ifs)
                throw std::runtime_error("Failed to read metadata: " + metaPath);
            ifs >> metadata;
            ifs.close();
        }

        std::vector<std::string> chunkHashes = metadata["chunks"];
        std::vector<char> fileData;
        for (const auto &hash : chunkHashes) {
            std::string chunkPath = chunksDir + hash;
            std::ifstream chunkFile(chunkPath, std::ios::binary);
            if (!chunkFile)
                throw std::runtime_error("Missing chunk: " + hash);
            std::vector<char> chunk((std::istreambuf_iterator<char>(chunkFile)), std::istreambuf_iterator<char>());
            fileData.insert(fileData.end(), chunk.begin(), chunk.end());
            chunkFile.close();
        }
        return fileData;
    }

    void updateFileData(const std::string& filename, const std::vector<char>& newData) {
        std::string metaPath = metadataDir + filename + ".json";

        // Check if file exists
        if (!fs::exists(metaPath)) {
            throw std::runtime_error("File not found: " + filename);
        }

        // Read existing metadata
        json existingMetadata;
        {
            std::ifstream ifs(metaPath);
            ifs >> existingMetadata;
            ifs.close();
        }

        // Compute new chunks
        std::vector<std::string> newChunks;
        ThreadPool pool(std::thread::hardware_concurrency());
        std::vector<std::future<std::string>> futures;

        size_t offset = 0;
        while (offset < newData.size()) {
            size_t bytesRead = std::min(CHUNK_SIZE, newData.size() - offset);
            std::vector<char> chunk(newData.begin() + offset, newData.begin() + offset + bytesRead);
            futures.push_back(pool.enqueue([this, chunk]() {
                std::string hash = computeHash(chunk);
                saveChunk(hash, chunk);  // Only saves if new
                return hash;
            }));
            offset += bytesRead;
        }

        for (auto& f : futures) {
            newChunks.push_back(f.get());
        }

        // Check for no changes
        if (existingMetadata["chunks"] == newChunks) {
            throw std::runtime_error("File identical - no changes required");
        }

        // Update reference counts
        json refCounts = readRefCounts();
        std::vector<std::string> oldChunks = existingMetadata["chunks"];

        // Decrement old chunks
        for (const auto& hash : oldChunks) {
            if (refCounts.find(hash) != refCounts.end()) {
                refCounts[hash] = refCounts[hash].get<int>() - 1;
                if (refCounts[hash] <= 0) {
                    refCounts.erase(hash);
                    fs::remove(chunksDir + hash);
                }
            }
        }

        // Increment new chunks
        for (const auto& hash : newChunks) {
            refCounts[hash] = refCounts.value(hash, 0) + 1;
        }

        writeRefCounts(refCounts);

        // Update metadata
        existingMetadata["chunks"] = newChunks;
        existingMetadata["size"] = newData.size();
        existingMetadata["updated_at"] = getTimestamp();

        std::ofstream ofs(metaPath);
        ofs << existingMetadata.dump(4);
        ofs.close();
    }
    void deleteFile(const std::string &filename) {
        std::string metaPath = metadataDir + filename + ".json";
        if (!fs::exists(metaPath))
            throw std::runtime_error("File not found: " + filename);

        json metadata;
        {
            std::ifstream ifs(metaPath);
            if (!ifs)
                throw std::runtime_error("Failed to read metadata: " + metaPath);
            ifs >> metadata;
            ifs.close();
        }

        fs::remove(metaPath);

        json refCounts = readRefCounts();
        for (const auto &hash : metadata["chunks"]) {
            std::string hashStr = hash.get<std::string>();
            if (refCounts.find(hashStr) != refCounts.end()) {
                int count = refCounts[hashStr];
                count--;
                if (count <= 0) {
                    refCounts.erase(hashStr);
                    std::string chunkPath = chunksDir + hashStr;
                    if (fs::exists(chunkPath))
                        fs::remove(chunkPath);
                } else {
                    refCounts[hashStr] = count;
                }
            }
        }
        writeRefCounts(refCounts);
        std::cout << "File " << filename << " deleted successfully" << std::endl;
    }
};

int main() {
    SimpleFileManager fm;
    httplib::Server server;

    // Upload file endpoint
    server.Post("/upload", [&fm](const httplib::Request &req, httplib::Response &res) {
        if (!req.has_file("file")) {
            res.status = 400;
            res.set_content("Missing file", "text/plain");
            return;
        }
        const auto& file = req.get_file_value("file");
        std::vector<char> data(file.content.begin(), file.content.end());
        try {
            fm.storeFileData(file.filename, data);
            res.set_content("File uploaded successfully", "text/plain");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(e.what(), "text/plain");
        }
    });

    // List files endpoint
    server.Get("/files", [&fm](const httplib::Request&, httplib::Response& res) {
        try {
            auto files = fm.listFiles();
            json j = files;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(e.what(), "text/plain");
        }
    });

    // Download file endpoint
    server.Get("/files/(.*)", [&fm](const httplib::Request& req, httplib::Response& res) {
        std::string filename = req.matches[1];
        try {
            auto data = fm.retrieveFile(filename);
            res.set_content(std::string(data.begin(), data.end()), "application/octet-stream");
            res.set_header("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        } catch (const std::exception& e) {
            res.status = 404;
            res.set_content(e.what(), "text/plain");
        }
    });

    // Delete file endpoint
    server.Delete("/files/(.*)", [&fm](const httplib::Request& req, httplib::Response& res) {
        std::string filename = req.matches[1];
        try {
            fm.deleteFile(filename);
            res.set_content("File deleted successfully", "text/plain");
        } catch (const std::exception& e) {
            res.status = 404;
            res.set_content(e.what(), "text/plain");
        }
    });


    server.Put("/files/(.*)", [&fm](const httplib::Request& req, httplib::Response& res) {
        std::string filename = req.matches[1];

        if (!req.has_file("file")) {
            res.status = 400;
            res.set_content("Missing file", "text/plain");
            return;
        }

        const auto& file = req.get_file_value("file");
        std::vector<char> newData(file.content.begin(), file.content.end());

        try {
            fm.updateFileData(filename, newData);
            res.set_content("File updated successfully", "text/plain");
        } catch (const std::exception& e) {
            if (std::string(e.what()) == "File identical - no changes required") {
                res.set_content(e.what(), "text/plain");
            } else {
                res.status = 500;
                res.set_content(e.what(), "text/plain");
            }
        }
    });

    std::cout << "Server running on http://localhost:8080" << std::endl;
    server.listen("0.0.0.0", 8080);

    return 0;
}
