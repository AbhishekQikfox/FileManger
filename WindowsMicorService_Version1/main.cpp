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
#include <ctime>
#include <string>
#include <vector>
#include <random>
#include <sstream>
#include <iomanip>
#include <openssl/sha.h>
#include <random>
#include <sstream>
#include <iomanip>


std::ofstream logFile;
std::ofstream perfFile;
// 64-bit random hex suffix (same as before)
static std::string randomHex64() {
    static std::random_device rd;
    static std::mt19937_64 eng(rd());
    uint64_t v = eng();
    std::ostringstream ss;
    ss << std::hex << std::setw(16) << std::setfill('0') << v;
    return ss.str();
}

// New: CID = SHA256( concat(chunkHashes) ) + "_" + randomHex64()
std::string generateCID(const std::vector<std::string>& chunkHashes) {
    // 1) build one big string
    std::string combined;
    combined.reserve(chunkHashes.size() * 64);
    for (auto& h : chunkHashes) combined += h;

    // 2) SHA256 it
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(combined.data()),
           combined.size(),
           digest);

    // 3) hex-encode the digest
    std::ostringstream hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        hex << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(digest[i]);
    }

    // 4) append suffix
    hex << "_" << randomHex64();
    return hex.str();
}


void WriteLog(const std::string& message) {
    if (logFile.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt) - 1] = 0; // Remove newline
        logFile << dt << " - " << message << std::endl;
        logFile.flush();
    }
}

void WritePerf(const std::string& message) {
    if (perfFile.is_open()) {
        time_t now = time(0);
        char* dt = ctime(&now);
        dt[strlen(dt) - 1] = 0; // Remove newline
        perfFile << dt << " - " << message << std::endl;
        perfFile.flush();
    }
}

template <typename Func>
void Benchmark(const std::string& label, Func func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    WritePerf("[Benchmark] " + label + ": " + std::to_string(duration) + " ms");
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
            fs::path chunksDir = "C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\windowService_2\\chunks";
            fs::path metadataDir = "C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\windowService_2\\metadata";
            fs::path refCountFile = "C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\windowService_2\\chunk_refs.json";

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

            // void incrementRefCount(const std::string &hash) {
            //     json refCounts = readRefCounts();
            //     refCounts[hash] = refCounts.value(hash, 0) + 1;
            //     writeRefCounts(refCounts);
            // }

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

            std::string storeFileData(const std::string& filename, const std::vector<char>& data) {
                std::vector<std::string> chunkHashes;
                size_t totalSize = data.size();
                ThreadPool pool(std::thread::hardware_concurrency());
                std::vector<std::future<std::string>> futures;

                // 1) chunk + hash in parallel
                size_t offset = 0;
                while (offset < data.size()) {
                    size_t bytesRead = std::min(CHUNK_SIZE, data.size() - offset);
                    std::vector<char> chunk(data.begin() + offset, data.begin() + offset + bytesRead);
                    futures.push_back(pool.enqueue([this, chunk]() {
                        auto hash = computeHash(chunk);
                        saveChunk(hash, chunk);
                        return hash;
                    }));
                    offset += bytesRead;
                }

                // 2) collect hashes
                for (auto& f : futures) {
                    chunkHashes.push_back(f.get());
                }

                // 3) update ref‐counts
                batchUpdateRefCounts(chunkHashes);

                // 4) generate CID + metadata path
                std::string cid = generateCID(chunkHashes);
                fs::path metaPath = metadataDir / (cid + ".json");

                // 5) guard against re‐upload of same CID
                if (fs::exists(metaPath)) {
                    throw std::runtime_error("File already exists with CID: " + cid);
                }

                // 6) write metadata
                json metadata;
                metadata["cid"]        = cid;
                metadata["filename"]   = filename;
                metadata["size"]       = totalSize;
                metadata["created_at"] = getTimestamp();
                metadata["chunks"]     = chunkHashes;

                std::ofstream ofs(metaPath.string());
                if (!ofs) {
                    throw std::runtime_error("Failed to write metadata: " + metaPath.string());
                }
                ofs << metadata.dump(4);
                ofs.close();

                WriteLog("File " + filename + " stored as CID=" + cid);
                return cid;
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

            // In SimpleFileManager, add:
json getMetadata(const std::string& cid) {
    fs::path metaPath = metadataDir / (cid + ".json");
    if (!fs::exists(metaPath))
        throw std::runtime_error("Metadata not found for CID: " + cid);
    std::ifstream ifs(metaPath.string());
    json m;
    ifs >> m;
    return m;
}

// In SimpleFileManager, replace retrieveFile with:
std::vector<char> retrieveByCID(const std::string& cid, std::string& outFilename) {
    // 1) metadata path is metadataDir/<cid>.json
    fs::path metaPath = metadataDir / (cid + ".json");
    if (!fs::exists(metaPath))
        throw std::runtime_error("CID not found: " + cid);

    // 2) read metadata
    json metadata;
    std::ifstream ifs(metaPath.string());
    if (!ifs)
        throw std::runtime_error("Failed to open metadata for CID: " + cid);
    ifs >> metadata;

    // 3) extract original filename
    outFilename = metadata.value("filename", cid);

    // 4) stream all chunks
    std::vector<char> fileData;
    for (auto& h : metadata["chunks"]) {
        fs::path chunkPath = chunksDir / h.get<std::string>();
        std::ifstream chunkFile(chunkPath.string(), std::ios::binary);
        if (!chunkFile)
            throw std::runtime_error("Missing chunk: " + h.get<std::string>());
        fileData.insert(
            fileData.end(),
            std::istreambuf_iterator<char>(chunkFile),
            std::istreambuf_iterator<char>()
        );
    }
    return fileData;
}


            // In SimpleFileManager.hpp / .cpp:

// Returns the new CID
// In SimpleFileManager class:

        // Returns the new CID after updating, or throws on identical or error
        std::string updateFileData(const std::string& oldCID, const std::vector<char>& newData) {
            // 1) Locate & read old metadata
            fs::path oldMetaPath = metadataDir / (oldCID + ".json");
            if (!fs::exists(oldMetaPath)) {
                throw std::runtime_error("CID not found: " + oldCID);
            }
            json oldMeta;
            {
                std::ifstream in{oldMetaPath.string()};
                in >> oldMeta;
            }
            auto filename  = oldMeta.at("filename").get<std::string>();
            auto oldChunks = oldMeta.at("chunks").get<std::vector<std::string>>();

            // 2) Chunk & hash newData in parallel
            ThreadPool pool{std::thread::hardware_concurrency()};
            std::vector<std::future<std::string>> futures;
            for (size_t off = 0; off < newData.size(); off += CHUNK_SIZE) {
                size_t len = std::min(CHUNK_SIZE, newData.size() - off);
                std::vector<char> chunk(newData.begin() + off, newData.begin() + off + len);
                futures.push_back(pool.enqueue([this, chunk]() {
                    auto h = computeHash(chunk);
                    saveChunk(h, chunk);
                    return h;
                }));
            }
            std::vector<std::string> newChunks;
            for (auto& f : futures) {
                newChunks.push_back(f.get());
            }

            // 3) No-op if identical
            if (newChunks == oldChunks) {
                throw std::runtime_error("File identical - no changes required");
            }

            // 4) Manually adjust ref-counts JSON
            json refCounts = readRefCounts();  // your existing single-arg reader
            // decrement old
            for (auto& h : oldChunks) {
                int cnt = refCounts.value(h, 0) - 1;
                if (cnt <= 0) {
                    refCounts.erase(h);
                    fs::remove(chunksDir / h);
                } else {
                    refCounts[h] = cnt;
                }
            }
            // increment new
            for (auto& h : newChunks) {
                refCounts[h] = refCounts.value(h, 0) + 1;
            }
            writeRefCounts(refCounts);  // your existing single-arg writer

            // 5) Generate new CID
            std::string newCID = generateCID(newChunks);

            // 6) Write new metadata file
            json newMeta;
            newMeta["cid"]        = newCID;
            newMeta["filename"]   = filename;
            newMeta["size"]       = newData.size();
            newMeta["created_at"] = oldMeta.value("created_at", getTimestamp());
            newMeta["updated_at"] = getTimestamp();
            newMeta["chunks"]     = newChunks;

            fs::path newMetaPath = metadataDir / (newCID + ".json");
            {
                std::ofstream out{newMetaPath.string()};
                if (!out) throw std::runtime_error("Failed to write metadata: " + newMetaPath.string());
                out << newMeta.dump(4);
            }

            // 7) Delete old metadata
            fs::remove(oldMetaPath);

            WriteLog("Updated CID " + oldCID + " → " + newCID);
            return newCID;
        }


        void deleteFile(const std::string& cid) {
            fs::path metaPath = metadataDir / (cid + ".json");

            if (!fs::exists(metaPath)) {
                throw std::runtime_error("File not found: " + cid);
            }

            // Load metadata to know which chunks this file used
            json metadata;
            std::ifstream ifs(metaPath);
            if (!ifs)
                throw std::runtime_error("Failed to read metadata: " + metaPath.string());
            ifs >> metadata;
            ifs.close();

            // Remove metadata file (first to avoid reusing this file before cleanup)
            fs::remove(metaPath);

            // Update reference counts
            json refCounts = readRefCounts();
            std::vector<std::string> chunks = metadata["chunks"];

            for (const auto& hash : chunks) {
                if (refCounts.contains(hash)) {
                    int count = refCounts[hash];
                    if (--count <= 0) {
                        refCounts.erase(hash);
                        fs::path chunkPath = chunksDir / hash;
                        if (fs::exists(chunkPath)) {
                            fs::remove(chunkPath);
                        }
                    } else {
                        refCounts[hash] = count;
                    }
                }
            }

            writeRefCounts(refCounts);
            WriteLog("File with CID " + cid + " deleted successfully");
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
            // Inside RunServer(), after adding getMetadata()
            server.Post("/upload", [&fm](const httplib::Request& req, httplib::Response& res) {
                WriteLog("Handling upload request");

                if (!req.has_file("file")) {
                    res.status = 400;
                    res.set_content("Missing file", "text/plain");
                    return;
                }

                auto file = req.get_file_value("file");
                std::vector<char> data(file.content.begin(), file.content.end());

                try {
                    std::string cid;
                    json metadata;

                    // Benchmark storing file
                    Benchmark("StoreFileData", [&]() {
                        cid = fm.storeFileData(file.filename, data);
                    });

                    // Benchmark fetching metadata
                    Benchmark("GetMetadata", [&]() {
                        metadata = fm.getMetadata(cid);
                    });

                    // Return response
                    res.set_header("Content-Type", "application/json");
                    res.set_content(metadata.dump(4), "application/json");
                }
                catch (const std::exception& e) {
                    res.status = 500;
                    res.set_content(e.what(), "text/plain");
                }
            });




            // List files endpoint
            server.Get("/files", [&fm](const httplib::Request&, httplib::Response& res) {
                WriteLog("Handling get request for file list ");
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
           // In RunServer(), replace your GET handler with:
            server.Get(R"(/files/([\w\-]+))", [&fm](const httplib::Request& req, httplib::Response& res) {
                std::string cid = req.matches[1];
                WriteLog("Handling download request for CID: " + cid);
                try {
                    std::string filename;
                    auto data = fm.retrieveByCID(cid, filename);

                    // send the raw bytes
                    res.set_content(std::string(data.begin(), data.end()), "application/octet-stream");
                    // use the original filename
                    res.set_header("Content-Disposition",
                                "attachment; filename=\"" + filename + "\"");
                } catch (const std::exception& e) {
                    res.status = 404;
                    res.set_content(e.what(), "text/plain");
                }
            });


            server.Delete(R"(/files/([\w\-]+))", [&fm](const httplib::Request& req, httplib::Response& res) {
                WriteLog("Handling delete request");

                std::string cid = req.matches[1];
                try {
                    fm.deleteFile(cid);
                    res.set_content("File deleted successfully", "text/plain");
                } catch (const std::exception& e) {
                    res.status = 404;
                    res.set_content(e.what(), "text/plain");
                }
            });


            // Update file endpoint
            // In RunServer(), replace your PUT handler with:
            server.Put(R"(/files/([\w\-]+))", [&fm](const httplib::Request& req, httplib::Response& res) {
                std::string oldCID = req.matches[1];
                WriteLog("Handling update request for CID: " + oldCID);

                if (!req.has_file("file")) {
                    res.status = 400;
                    res.set_content("Missing file", "text/plain");
                    return;
                }

                auto fv = req.get_file_value("file");
                std::vector<char> newData(fv.content.begin(), fv.content.end());

                try {
                    // 1) Perform the update; returns the new CID
                    std::string newCID = fm.updateFileData(oldCID, newData);

                    // 2) Fetch full metadata via the public API
                    json metadata = fm.getMetadata(newCID);

                    // 3) Return it
                    res.set_header("Content-Type", "application/json");
                    res.set_content(metadata.dump(4), "application/json");
                }
                catch (const std::exception& e) {
                    std::string msg = e.what();
                    if (msg == "File identical - no changes required") {
                        res.status = 200;
                        res.set_content(msg, "text/plain");
                    } else {
                        res.status = 500;
                        res.set_content(msg, "text/plain");
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
            paused = true;

            serviceStatus.dwCurrentState = SERVICE_PAUSED;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            WriteLog("service paused");
            break;
        case SERVICE_CONTROL_CONTINUE:
            WriteLog("Continue request received from SCM");
            serviceStatus.dwCurrentState = SERVICE_CONTINUE_PENDING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            paused = false;
            serviceStatus.dwCurrentState = SERVICE_RUNNING;
            SetServiceStatus(serviceStatusHandle, &serviceStatus);
            WriteLog("service Running again");
            break;
        default:
            break;
    }
}

void WINAPI ServiceMain(DWORD argc, LPSTR *argv){
    serviceStatusHandle = RegisterServiceCtrlHandler("MyBasicService", ServiceCtrlHandler);
    if (!serviceStatusHandle) {
        WriteLog("Serive not registred succesgully");
        return;
    }
    serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    serviceStatus.dwCurrentState = SERVICE_START_PENDING;
    serviceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PAUSE_CONTINUE;
    serviceStatus.dwWin32ExitCode = 0;
    serviceStatus.dwServiceSpecificExitCode = 0;
    serviceStatus.dwCheckPoint = 0;
    serviceStatus.dwWaitHint = 3000;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
    WriteLog("Service starting");
    running = true;
    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    serviceStatus.dwCheckPoint = 0;
    serviceStatus.dwWaitHint = 0;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
    WriteLog("Service started successfully");

    std::thread serverThread(RunServer);

    {
        std::unique_lock<std::mutex> lock(serverMutex);
        serverCV.wait(lock, []{ return serverPtr != nullptr; });
    }

    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    WriteLog("Main service loop ended, waiting for server thread");
    if (serverThread.joinable()) {
        serverThread.join();
    }

    WriteLog("Service shutting down");
    serviceStatus.dwCurrentState = SERVICE_STOPPED;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
}

int main(){
    logFile.open("C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\windowService_2\\myservice.log", std::ios::app);
    perfFile.open("C:\\Users\\Training\\Desktop\\Multithreading-Cpp-Course-main\\windowService_2\\benchmark.log", std::ios::app);
if (!perfFile.is_open()) {
    std::cerr << "Failed to open benchmark log file!" << std::endl;
}

    WriteLog("Creating service table");
    SERVICE_TABLE_ENTRY serviceTable[]={
        {"MyBasicService",ServiceMain},
        {NULL,NULL}
    };
    WriteLog("service table created and calling dispatcher");
    StartServiceCtrlDispatcher(serviceTable);
    WriteLog("main loop ending ");
    return 0;

}
