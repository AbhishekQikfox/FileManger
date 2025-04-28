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
#include <windows.h>
#include <winsvc.h>
#include <ctime>
#include <csignal>

std::ofstream logFile;

void WriteLog(const std::string& message) {
    if (logFile.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt) - 1] = 0; // Remove newline
        logFile << dt << " - " << message << std::endl;
        logFile.flush();
    }
}

namespace fs = std::filesystem;
using json = nlohmann::json;

SERVICE_STATUS serviceStatus;
SERVICE_STATUS_HANDLE serviceStatusHandle;
static std::atomic<bool> paused(false);
std::atomic<bool> running(false);
// Allow RunServer() to see the paused flag
extern std::atomic<bool> paused;

std::mutex serverMutex;
std::condition_variable serverCV;
httplib::Server* serverPtr = nullptr;


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
        std::mutex refCountMutex; // Dedicated mutex for reference counting
        fs::path chunksDir = "C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\File_Version_2\\chunks";
        fs::path metadataDir = "C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\File_Version_2\\metadata";
        fs::path refCountFile = "C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\File_Version_2\\chunk_refs.json";

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
            std::lock_guard<std::mutex> lock(refCountMutex);
            json refCounts;
            if (fs::exists(refCountFile)) {
                std::ifstream ifs(refCountFile.string());
                if (ifs.is_open()) {
                    ifs >> refCounts;
                    ifs.close();
                } else {
                    WriteLog("Warning: Failed to open reference count file for reading");
                    refCounts = json::object();
                }
            }
            return refCounts.empty() ? json::object() : refCounts;
        }

        void writeRefCounts(const json &refCounts) {
            std::lock_guard<std::mutex> lock(refCountMutex);
            std::ofstream ofs(refCountFile.string());
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

        // New batch update method for efficiency
        void batchUpdateRefCounts(const std::vector<std::string> &hashes) {
            json refCounts = readRefCounts();
            for (const auto &hash : hashes) {
                refCounts[hash] = refCounts.value(hash, 0) + 1;
            }
            writeRefCounts(refCounts);
            WriteLog("Updated reference counts for " + std::to_string(hashes.size()) + " chunks");
        }

        void saveChunk(const std::string &hash, const std::vector<char> &chunk) {
            fs::path chunkPath = chunksDir / hash;
            if (!fs::exists(chunkPath)) {
                std::lock_guard<std::mutex> lock(chunkMutex);
                if (!fs::exists(chunkPath)) {
                    std::ofstream ofs(chunkPath.string(), std::ios::binary);
                    if (!ofs)
                        throw std::runtime_error("Failed to open chunk file: " + chunkPath.string());
                    ofs.write(chunk.data(), chunk.size());
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
            fs::path metaPath = metadataDir / (filename + ".json");
            if (fs::exists(metaPath))
                throw std::runtime_error("File already exists: " + filename);

            std::vector<std::string> chunkHashes;
            size_t totalSize = data.size();
            ThreadPool pool(std::thread::hardware_concurrency());
            std::vector<std::future<std::string>> futures;

            size_t offset = 0;
            while (offset < data.size()) {
                size_t bytesRead = std::min(CHUNK_SIZE, data.size() - offset);
                std::vector<char> chunk(data.begin() + offset, data.begin() + offset + bytesRead);
                futures.push_back(pool.enqueue([this, chunk]() {
                    std::string hash = computeHash(chunk);
                    saveChunk(hash, chunk);
                    return hash;
                }));
                offset += bytesRead;
            }

            for (auto &f : futures) {
                chunkHashes.push_back(f.get());
            }

            // Batch update reference counts after all chunks are processed
            batchUpdateRefCounts(chunkHashes);

            json metadata;
            metadata["filename"] = filename;
            metadata["size"] = totalSize;
            metadata["created_at"] = getTimestamp();
            metadata["chunks"] = chunkHashes;

            std::ofstream ofs(metaPath.string());
            if (!ofs)
                throw std::runtime_error("Failed to write metadata: " + metaPath.string());
            ofs << metadata.dump(4);
            ofs.close();
            WriteLog("File " + filename + " stored successfully");
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
            fs::path metaPath = metadataDir / (filename + ".json");
            if (!fs::exists(metaPath))
                throw std::runtime_error("File not found: " + filename);

            json metadata;
            std::ifstream ifs(metaPath.string());
            if (!ifs)
                throw std::runtime_error("Failed to read metadata: " + metaPath.string());
            ifs >> metadata;
            ifs.close();

            std::vector<std::string> chunkHashes = metadata["chunks"];
            std::vector<char> fileData;
            for (const auto &hash : chunkHashes) {
                fs::path chunkPath = chunksDir / hash;
                std::ifstream chunkFile(chunkPath.string(), std::ios::binary);
                if (!chunkFile)
                    throw std::runtime_error("Missing chunk: " + hash);
                std::vector<char> chunk((std::istreambuf_iterator<char>(chunkFile)), std::istreambuf_iterator<char>());
                fileData.insert(fileData.end(), chunk.begin(), chunk.end());
                chunkFile.close();
            }
            return fileData;
        }

        void updateFileData(const std::string& filename, const std::vector<char>& newData) {
            fs::path metaPath = metadataDir / (filename + ".json");
            if (!fs::exists(metaPath))
                throw std::runtime_error("File not found: " + filename);

            json existingMetadata;
            std::ifstream ifs(metaPath.string());
            ifs >> existingMetadata;
            ifs.close();

            std::vector<std::string> newChunks;
            ThreadPool pool(std::thread::hardware_concurrency());
            std::vector<std::future<std::string>> futures;

            size_t offset = 0;
            while (offset < newData.size()) {
                size_t bytesRead = std::min(CHUNK_SIZE, newData.size() - offset);
                std::vector<char> chunk(newData.begin() + offset, newData.begin() + offset + bytesRead);
                futures.push_back(pool.enqueue([this, chunk]() {
                    std::string hash = computeHash(chunk);
                    saveChunk(hash, chunk);
                    return hash;
                }));
                offset += bytesRead;
            }

            for (auto& f : futures) {
                newChunks.push_back(f.get());
            }

            if (existingMetadata["chunks"] == newChunks) {
                throw std::runtime_error("File identical - no changes required");
            }

            json refCounts = readRefCounts();
            std::vector<std::string> oldChunks = existingMetadata["chunks"];

            // Decrement old chunks
            for (const auto& hash : oldChunks) {
                if (refCounts.find(hash) != refCounts.end()) {
                    refCounts[hash] = refCounts[hash].get<int>() - 1;
                    if (refCounts[hash] <= 0) {
                        refCounts.erase(hash);
                        fs::remove(chunksDir / hash);
                    }
                }
            }

            // Increment new chunks
            for (const auto& hash : newChunks) {
                refCounts[hash] = refCounts.value(hash, 0) + 1;
            }

            writeRefCounts(refCounts);

            existingMetadata["chunks"] = newChunks;
            existingMetadata["size"] = newData.size();
            existingMetadata["updated_at"] = getTimestamp();

            std::ofstream ofs(metaPath.string());
            ofs << existingMetadata.dump(4);
            ofs.close();
            WriteLog("File " + filename + " updated successfully");
        }

        void deleteFile(const std::string &filename) {
            fs::path metaPath = metadataDir / (filename + ".json");
            if (!fs::exists(metaPath))
                throw std::runtime_error("File not found: " + filename);

            json metadata;
            std::ifstream ifs(metaPath.string());
            if (!ifs)
                throw std::runtime_error("Failed to read metadata: " + metaPath.string());
            ifs >> metadata;
            ifs.close();

            fs::remove(metaPath);

            json refCounts = readRefCounts();
            for (const auto &hash : metadata["chunks"]) {
                std::string hashStr = hash.get<std::string>();
                if (refCounts.find(hashStr) != refCounts.end()) {
                    int count = refCounts[hashStr];
                    count--;
                    if (count <= 0) {
                        refCounts.erase(hashStr);
                        fs::path chunkPath = chunksDir / hashStr;
                        if (fs::exists(chunkPath))
                            fs::remove(chunkPath);
                    } else {
                        refCounts[hashStr] = count;
                    }
                }
            }
            writeRefCounts(refCounts);
            WriteLog("File " + filename + " deleted successfully");
        }


    };


    void RunServer() {
        SimpleFileManager fm;
        httplib::Server server;

        server.set_pre_routing_handler(
                [](const httplib::Request &req, httplib::Response &res) {
                    if (paused.load()) {
                        res.status = 503;
                        res.set_content("Service is paused", "text/plain");
                      // tell httplib “we handled it, don’t try your normal routes”
                     return httplib::Server::HandlerResponse::Handled;
                  }
                   return httplib::Server::HandlerResponse::Unhandled;
               }
            );

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

        // Update file endpoint
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


        {
            std::lock_guard<std::mutex> lock(serverMutex);
            serverPtr = &server;
        }
        serverCV.notify_one();

        WriteLog("Server running on http://localhost:8080");
        server.listen("0.0.0.0", 8080);
    }


    void WINAPI ServiceCtrlHandler(DWORD controlCode) {
        switch (controlCode) {
            case SERVICE_CONTROL_STOP:
            WriteLog("Stop request received from SCM");
                running = false;
                serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
                SetServiceStatus(serviceStatusHandle, &serviceStatus);

                // Stop the HTTP server if it's running
                {
                    std::lock_guard<std::mutex> lock(serverMutex);
                    if (serverPtr != nullptr) {
                        WriteLog("Stopping HTTP server");
                        serverPtr->stop();
                    }
                }
                break;
            case SERVICE_CONTROL_PAUSE:
                         WriteLog("Pause request received from SCM");
                         serviceStatus.dwCurrentState = SERVICE_PAUSE_PENDING;
                         SetServiceStatus(serviceStatusHandle, &serviceStatus);

                         // your pause logic: e.g. set a flag, pause loops or stop server
                        paused = true;

                serviceStatus.dwCurrentState = SERVICE_PAUSED;
                       SetServiceStatus(serviceStatusHandle, &serviceStatus);
                       break;

                    case SERVICE_CONTROL_CONTINUE:
                       WriteLog("Continue request received from SCM");
                           serviceStatus.dwCurrentState = SERVICE_CONTINUE_PENDING;
                           SetServiceStatus(serviceStatusHandle, &serviceStatus);

                // resume logic
                           paused = false;

                            serviceStatus.dwCurrentState = SERVICE_RUNNING;
                            SetServiceStatus(serviceStatusHandle, &serviceStatus);
                        break;

            default:
                break;
        }
    }


    void WINAPI ServiceMain(DWORD argc, LPTSTR *argv) {
        // Initialize service status handlers and set initial state
        serviceStatusHandle = RegisterServiceCtrlHandler("MyBasicService", ServiceCtrlHandler);
        if (!serviceStatusHandle) {
            return;
        }

        // Initialize service status structure
        serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
        serviceStatus.dwCurrentState = SERVICE_START_PENDING;
         serviceStatus.dwControlsAccepted        = SERVICE_ACCEPT_STOP
                                         | SERVICE_ACCEPT_PAUSE_CONTINUE;
        serviceStatus.dwWin32ExitCode = 0;
        serviceStatus.dwServiceSpecificExitCode = 0;
        serviceStatus.dwCheckPoint = 0;
        serviceStatus.dwWaitHint = 3000; // Give 3 seconds for startup
        SetServiceStatus(serviceStatusHandle, &serviceStatus);

        // Setup logging
        logFile.open("C:\\Windows\\Temp\\myservice.log", std::ios::app);
        if (!logFile) {
            serviceStatus.dwCurrentState = SERVICE_STOPPED;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            return;
        }

        WriteLog("Service starting");

        // Set service to running state
        running = true;
        serviceStatus.dwCurrentState = SERVICE_RUNNING;
        serviceStatus.dwCheckPoint = 0;
        serviceStatus.dwWaitHint = 0;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);

        WriteLog("Service started successfully");

        // Create file manager instance
        SimpleFileManager fm;

        // Start HTTP server in separate thread
        std::thread serverThread(RunServer);

        // Wait for server to start
        {
            std::unique_lock<std::mutex> lock(serverMutex);
            serverCV.wait(lock, []{ return serverPtr != nullptr; });
        }

        // Main service loop - periodically check if we should continue running
        while (running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        WriteLog("Main service loop ended, waiting for server thread");

        // Wait for server thread to finish
        if (serverThread.joinable()) {
            serverThread.join();
        }

        WriteLog("Service shutting down");
        logFile.close();

        // Set service status to stopped
        serviceStatus.dwCurrentState = SERVICE_STOPPED;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
    }

void SignalHandler(int signal) {
    if (signal == SIGINT) {
        running = false;
    }
}


int main() {
    SERVICE_TABLE_ENTRY serviceTable[] = {
        {"MyBasicService", ServiceMain},
        {NULL, NULL}
    };
    if (!StartServiceCtrlDispatcher(serviceTable)) {
        if (GetLastError() == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
            logFile.open("C:\\Windows\\Temp\\myservice.log", std::ios::app);
            if (!logFile) {
                std::cerr << "Failed to open log file" << std::endl;
                return 1;
            }
            running = true;
            signal(SIGINT, SignalHandler);
            while (running) {
                WriteLog("Service is running (console mode)");
                Sleep(5000);
            }
            logFile.close();
        } else {
            std::cerr << "Failed to start service" << std::endl;
            return 1;
        }
    }
    return 0;
}
