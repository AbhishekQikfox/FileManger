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
#include <nlohmann/json.hpp>
#include <sstream>
#include <algorithm>
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"
#include <windows.h>
#include <ctime>
#include <random>
#include <openssl/sha.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>

using json = nlohmann::json;
namespace fs = std::filesystem;

// Global variables
std::ofstream logFile;
std::mutex logMutex;
size_t CHUNK_SIZE;
std::string BACKEND_HOST;
int BACKEND_PORT;
int SERVER_PORT;
std::string LOG_FILE_PATH;

void WriteLog(const std::string& message) {
    std::lock_guard<std::mutex> lock(logMutex);
    if (logFile.is_open()) {
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now.time_since_epoch()) % 1000;
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&now_time_t), "%Y-%m-%d %H:%M:%S")
            << '.' << std::setfill('0') << std::setw(3) << now_ms.count();
        logFile << oss.str() << " - " << message << std::endl;
        logFile.flush();
    }
}

json loadConfig(const std::string& configPath) {
    std::ifstream configFile(configPath);
    if (!configFile.is_open()) {
        std::cerr << "Failed to open config file: " << configPath << std::endl;
        return json::object();
    }
    json config;
    try {
        configFile >> config;
    } catch (const json::parse_error& e) {
        std::cerr << "Failed to parse config file: " << e.what() << std::endl;
        return json::object();
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

std::vector<char> base64Decode(const std::string& encoded) {
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO* bmem = BIO_new_mem_buf(encoded.data(), static_cast<int>(encoded.size()));
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bmem = BIO_push(b64, bmem);
    std::vector<char> buffer(encoded.size());
    int decodedLen = BIO_read(bmem, buffer.data(), static_cast<int>(buffer.size()));
    BIO_free_all(bmem);
    if (decodedLen > 0) {
        buffer.resize(decodedLen);
    } else {
        buffer.clear();
    }
    return buffer;
}

static std::string randomHex64() {
    static std::random_device rd;
    static std::mt19937_64 eng(rd());
    uint64_t v = eng();
    std::ostringstream ss;
    ss << std::hex << std::setw(16) << std::setfill('0') << v;
    return ss.str();
}

std::string generateCID(const std::vector<std::string>& chunkHashes) {
    std::string combined;
    combined.reserve(chunkHashes.size() * 64);
    for (auto& h : chunkHashes) combined += h;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(combined.data()), combined.size(), digest);
    std::ostringstream hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        hex << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
    }
    hex << "_" << randomHex64();
    return hex.str();
}

SERVICE_STATUS serviceStatus;
SERVICE_STATUS_HANDLE serviceStatusHandle;
static std::atomic<bool> paused(false);
std::atomic<bool> running(false);
std::mutex serverMutex;
std::condition_variable serverCV;
httplib::SSLServer* serverPtr = nullptr;

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

std::string computeHash(const std::vector<char>& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char*)data.data(), data.size(), hash);
    std::string hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        char buf[3];
        sprintf(buf, "%02x", hash[i]);
        hex += buf;
    }
    return hex;
}

std::pair<std::vector<char>, std::string> GetFileFromBackend(const std::string& cid) {
    WriteLog("Retrieving file with CID: " + cid + " from backend");
    httplib::Client client("https://" + BACKEND_HOST + ":" + std::to_string(BACKEND_PORT));
    client.set_connection_timeout(10);
    client.set_read_timeout(60);
    // Disable certificate verification for localhost testing (remove in production)
    client.enable_server_certificate_verification(false);
    auto res = client.Get("/file/" + cid);
    if (!res) {
        WriteLog("Failed to connect to backend service");
        return {{}, ""};
    }
    if (res->status != 200) {
        WriteLog("Backend service returned error: " + std::to_string(res->status) + " - " + res->body);
        return {{}, ""};
    }
    std::string filename = "download.bin";
    if (res->has_header("Content-Disposition")) {
        std::string disposition = res->get_header_value("Content-Disposition");
        size_t pos = disposition.find("filename=\"");
        if (pos != std::string::npos) {
            pos += 10;
            size_t end = disposition.find("\"", pos);
            if (end != std::string::npos) {
                filename = disposition.substr(pos, end - pos);
            }
        }
    }
    std::vector<char> fileData(res->body.begin(), res->body.end());
    WriteLog("Successfully retrieved file with CID: " + cid + " (" + std::to_string(fileData.size()) + " bytes)");
    return {fileData, filename};
}

bool SendToBackendHTTP(const httplib::MultipartFormDataItems& items) {
    WriteLog("Connecting to backend service at " + BACKEND_HOST + ":" + std::to_string(BACKEND_PORT));
    
    // Log the items being sent for debugging
    WriteLog("Sending multipart form data items:");
    for (const auto& item : items) {
        std::string log = "  name: " + item.name + ", content_type: " + item.content_type;
        if (!item.filename.empty()) {
            log += ", filename: " + item.filename;
        }
        log += ", content_length: " + std::to_string(item.content.size());
        WriteLog(log);
    }

    httplib::Client client("https://" + BACKEND_HOST + ":" + std::to_string(BACKEND_PORT));
    client.set_connection_timeout(10);
    client.set_read_timeout(60);
    // Disable certificate verification for localhost testing (remove in production)
    client.enable_server_certificate_verification(false);
    
    auto res = client.Post("/store", items);
    
    if (!res) {
        WriteLog("Failed to connect to backend service");
        return false;
    }
    if (res->status != 200) {
        WriteLog("Backend service returned error: " + std::to_string(res->status) + " - " + res->body);
        return false;
    }
    
    WriteLog("Successfully sent data to backend. Response: " + res->body);
    return true;
}

void RunServer() {
    // Define paths to certificate and key files
    const std::string cert_path = "C:\\Users\\Training\\Desktop\\ProjectRoot\\cert.pem";
    const std::string key_path = "C:\\Users\\Training\\Desktop\\ProjectRoot\\key.pem";

    // Initialize SSLServer instead of Server
    httplib::SSLServer server(cert_path.c_str(), key_path.c_str());
    server.Post("/upload", [](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_file("file")) {
            res.status = 400;
            res.set_content("Missing 'file' in upload", "text/plain");
            WriteLog("Upload rejected: Missing 'file' field");
            return;
        }
        
        const auto& file = req.get_file_value("file");
        WriteLog("Received upload: " + file.filename + " (" + std::to_string(file.content.size()) + " bytes)");
        
        unsigned int numThreads = std::thread::hardware_concurrency();
        if (numThreads == 0) numThreads = 4;
        ThreadPool pool(numThreads);
        WriteLog("Created thread pool with " + std::to_string(numThreads) + " threads");
        
        std::vector<std::string> chunkHashes;
        std::vector<std::vector<char>> chunks;
        std::vector<std::future<std::string>> futureHashes;
        
        std::vector<char> data(file.content.begin(), file.content.end());
        size_t totalChunks = (data.size() + CHUNK_SIZE - 1) / CHUNK_SIZE;
        chunkHashes.resize(totalChunks);
        chunks.resize(totalChunks);
        
        WriteLog("Processing file in " + std::to_string(totalChunks) + " chunks");
        
        for (size_t i = 0; i < totalChunks; i++) {
            size_t offset = i * CHUNK_SIZE;
            size_t len = std::min(CHUNK_SIZE, data.size() - offset);
            chunks[i] = std::vector<char>(data.begin() + offset, data.begin() + offset + len);
            auto task = [chunk = chunks[i]]() -> std::string {
                return computeHash(chunk);
            };
            futureHashes.push_back(pool.enqueue(task));
        }
        
        for (size_t i = 0; i < totalChunks; i++) {
            chunkHashes[i] = futureHashes[i].get();
        }
        
        WriteLog("All chunks hashed in parallel");
        
        std::string cid = generateCID(chunkHashes);
        WriteLog("Generated CID: " + cid);
        
        httplib::MultipartFormDataItems items;
        // Add form fields explicitly as parameters with no filename
        items.push_back({"cid", cid, "", "text/plain"});
        items.push_back({"original_filename", file.filename, "", "text/plain"});
        items.push_back({"chunk_hashes", json(chunkHashes).dump(), "", "application/json"});
        
        // Add chunks as files
        for (size_t i = 0; i < totalChunks; ++i) {
            std::string chunk_name = "chunk_" + std::to_string(i);
            std::string chunk_data(chunks[i].begin(), chunks[i].end());
            std::string filename = chunk_name + ".bin";
            items.push_back({chunk_name, chunk_data, filename, "application/octet-stream"});
        }
        
        WriteLog("Sending multipart/form-data to backend");
        if (!SendToBackendHTTP(items)) {
            WriteLog("Failed to send to backend");
            res.status = 500;
            res.set_content("Failed to send to backend", "text/plain");
            return;
        }
        
        WriteLog("Successfully sent to backend. Returning CID to client.");
        res.status = 200;
        res.set_content(cid, "text/plain");
    });

    server.Get("/files/:cid", [](const httplib::Request& req, httplib::Response& res) {
        std::string cid = req.path_params.at("cid");
        auto [fileData, filename] = GetFileFromBackend(cid);
        if (fileData.empty()) {
            res.status = 404;
            res.set_content("File not found", "text/plain");
            return;
        }
        res.status = 200;
        res.set_header("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        res.set_content(std::string(fileData.begin(), fileData.end()), "application/octet-stream");
    });

    server.Put("/update/:cid", [](const httplib::Request& req, httplib::Response& res) {
        std::string old_cid = req.path_params.at("cid");
        if (!req.has_file("file")) {
            res.status = 400;
            res.set_content("Missing 'file' in update", "text/plain");
            return;
        }
        const auto& file = req.get_file_value("file");
        std::vector<char> new_data(file.content.begin(), file.content.end());
        std::vector<std::string> new_chunk_hashes;
        std::vector<std::string> new_encoded_chunks;
        size_t offset = 0;
        while (offset < new_data.size()) {
            size_t len = std::min(static_cast<size_t>(CHUNK_SIZE), new_data.size() - offset);
            std::vector<char> chunk(new_data.begin() + offset, new_data.begin() + offset + len);
            std::string hash = computeHash(chunk);
            new_chunk_hashes.push_back(hash);
            std::string encoded = base64Encode(chunk);
            new_encoded_chunks.push_back(encoded);
            offset += len;
        }
        std::string new_cid = generateCID(new_chunk_hashes);
        json payload;
        payload["old_cid"] = old_cid;
        payload["new_cid"] = new_cid;
        payload["new_chunks"] = new_chunk_hashes;
        payload["new_data"] = new_encoded_chunks;
        payload["new_filename"] = file.filename;
        std::string json_str = payload.dump();
        httplib::Client client("https://" + BACKEND_HOST + ":" + std::to_string(BACKEND_PORT));
        client.enable_server_certificate_verification(false); // For testing
        auto backend_res = client.Post("/update", json_str, "application/json");
        if (backend_res && backend_res->status == 200) {
            res.status = 200;
            res.set_content(backend_res->body, "text/plain");
        } else {
            res.status = backend_res ? backend_res->status : 500;
            res.set_content("Update failed", "text/plain");
        }
    });

    server.Delete("/files/:cid", [](const httplib::Request& req, httplib::Response& res) {
        std::string cid = trim(req.path_params.at("cid"));
        WriteLog("Received DELETE request for CID: " + cid + ", hex: " + stringToHex(cid));
        httplib::Client client("https://" + BACKEND_HOST + ":" + std::to_string(BACKEND_PORT));
        client.set_connection_timeout(10);
        client.set_read_timeout(60);
        client.enable_server_certificate_verification(false); // For testing
        WriteLog("Sending DELETE request to backend for CID: " + cid);
        auto backend_res = client.Delete("/file/" + cid);
        if (backend_res && backend_res->status == 200) {
            WriteLog("Successfully deleted CID: " + cid + ", backend response: " + backend_res->body);
            res.status = 200;
            res.set_content("Deleted", "text/plain");
        } else {
            std::string error_msg = "Failed to delete CID: " + cid;
            if (backend_res) {
                error_msg += ", status: " + std::to_string(backend_res->status) + ", response: " + backend_res->body;
            } else {
                error_msg += ", no response from backend";
            }
            WriteLog(error_msg);
            res.status = backend_res ? backend_res->status : 500;
            res.set_content("Delete failed: " + (backend_res ? backend_res->body : "No response"), "text/plain");
        }
    });

    {
        std::lock_guard<std::mutex> lock(serverMutex);
        serverPtr = &server;
        serverCV.notify_one();
    }

    WriteLog("Server started on port " + std::to_string(SERVER_PORT) + " with HTTPS");
    if (!server.listen("0.0.0.0", SERVER_PORT)) {
        WriteLog("Failed to start HTTPS server on port " + std::to_string(SERVER_PORT));
    }
    WriteLog("Server stopped");
}

void WINAPI ServiceCtrlHandler(DWORD controlCode) {
    switch (controlCode) {
    case SERVICE_CONTROL_STOP:
        running = false;
        serviceStatus.dwCurrentState = SERVICE_STOP_PENDING;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        {
            std::lock_guard<std::mutex> lock(serverMutex);
            if (serverPtr) serverPtr->stop();
        }
        break;
    case SERVICE_CONTROL_PAUSE:
        paused = true;
        serviceStatus.dwCurrentState = SERVICE_PAUSED;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        break;
    case SERVICE_CONTROL_CONTINUE:
        paused = false;
        serviceStatus.dwCurrentState = SERVICE_RUNNING;
        SetServiceStatus(serviceStatusHandle, &serviceStatus);
        break;
    }
}

void WINAPI ServiceMain(DWORD argc, LPSTR* argv) {
    serviceStatusHandle = RegisterServiceCtrlHandler("Processsing", ServiceCtrlHandler);
    serviceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    serviceStatus.dwCurrentState = SERVICE_START_PENDING;
    serviceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PAUSE_CONTINUE;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
    running = true;
    serviceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
    std::thread serverThread(RunServer);
    {
        std::unique_lock<std::mutex> lock(serverMutex);
        serverCV.wait(lock, [] { return serverPtr != nullptr; });
    }
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (serverThread.joinable()) serverThread.join();
    serviceStatus.dwCurrentState = SERVICE_STOPPED;
    SetServiceStatus(serviceStatusHandle, &serviceStatus);
}

int main() {
    std::string configPath = "C:\\Users\\Training\\Desktop\\ProjectRoot\\FrontendService\\config.json";
    json config = loadConfig(configPath);

    CHUNK_SIZE = config.value("chunk_size", 1024 * 1024);
    BACKEND_HOST = config.value("backend_host", "127.0.0.1");
    BACKEND_PORT = config.value("backend_port", 8081);
    SERVER_PORT = config.value("server_port", 8080);
    LOG_FILE_PATH = config.value("log_file", "C:\\Users\\Training\\Desktop\\ProjectRoot\\FrontendService\\frontend.log");

    logFile.open(LOG_FILE_PATH, std::ios::app);
    WriteLog("Frontend service starting with config: " + config.dump());

    SERVICE_TABLE_ENTRY serviceTable[] = {
        {"Processsing", ServiceMain},
        {NULL, NULL}
    };

    if (!StartServiceCtrlDispatcher(serviceTable)) {
        DWORD error = GetLastError();
        if (error == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
            WriteLog("Running as standalone application (not as service)");
            running = true;
            RunServer();
            return 0;
        }
        WriteLog("Failed to start service. Error: " + std::to_string(error));
    }

    return 0;
}