#include <gtest/gtest.h>
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"
#include <string>
#include <fstream>
#include <filesystem>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace fs = std::filesystem;

const std::string FRONTEND_HOST = "127.0.0.1";
const int FRONTEND_PORT = 8080;

class IntegrationTest : public ::testing::Test {
protected:
    httplib::SSLClient cli;

    IntegrationTest() : cli(FRONTEND_HOST, FRONTEND_PORT) {
        cli.enable_server_certificate_verification(false); // Match Postman behavior
        cli.set_connection_timeout(10);
        cli.set_read_timeout(60);
    }

    std::string createTempFile(const std::string& content, const std::string& filename) {
        std::string filepath = filename;
        std::ofstream outfile(filepath, std::ios::binary);
        outfile << content;
        outfile.close();
        return filepath;
    }

    void removeFile(const std::string& filepath) {
        fs::remove(filepath);
    }
};

TEST_F(IntegrationTest, UploadAndDownload) {
    std::string test_content = "Test file content for upload and download.";
    std::string test_filename = "test_upload.txt";
    std::string test_file_path = createTempFile(test_content, test_filename);

    httplib::MultipartFormDataItems items = {
        {"file", test_content, test_filename, "text/plain"}
    };
    auto res = cli.Post("/upload", items);
    ASSERT_TRUE(res != nullptr) << "Upload request failed";
    EXPECT_EQ(res->status, 200) << "Upload failed with status: " << (res ? res->status : -1);
    std::string cid = res->body;

    res = cli.Get("/files/" + cid);
    ASSERT_TRUE(res != nullptr) << "Download request failed";
    EXPECT_EQ(res->status, 200) << "Download failed with status: " << res->status;
    EXPECT_EQ(res->body, test_content) << "Downloaded content does not match uploaded content";

    removeFile(test_file_path);
}

TEST_F(IntegrationTest, UpdateFile) {
    std::string initial_content = "Initial file content.";
    std::string test_filename = "test_update.txt";
    std::string test_file_path = createTempFile(initial_content, test_filename);

    httplib::MultipartFormDataItems items = {
        {"file", initial_content, test_filename, "text/plain"}
    };
    auto res = cli.Post("/upload", items);
    ASSERT_TRUE(res != nullptr) << "Initial upload request failed";
    EXPECT_EQ(res->status, 200) << "Initial upload failed with status: " << res->status;
    std::string initial_cid = res->body;

    std::string updated_content = "Updated file content.";
    items = {
        {"file", updated_content, test_filename, "text/plain"}
    };
    res = cli.Put("/update/" + initial_cid, items);
    ASSERT_TRUE(res != nullptr) << "Update request failed";
    EXPECT_EQ(res->status, 200) << "Update failed with status: " << res->status;
    std::string new_cid = res->body;

    res = cli.Get("/files/" + new_cid);
    ASSERT_TRUE(res != nullptr) << "Download updated file request failed";
    EXPECT_EQ(res->status, 200) << "Download updated file failed with status: " << res->status;
    EXPECT_EQ(res->body, updated_content) << "Downloaded updated content does not match";

    removeFile(test_file_path);
}

TEST_F(IntegrationTest, DeleteFile) {
    std::string test_content = "File content to delete.";
    std::string test_filename = "test_delete.txt";
    std::string test_file_path = createTempFile(test_content, test_filename);

    httplib::MultipartFormDataItems items = {
        {"file", test_content, test_filename, "text/plain"}
    };
    auto res = cli.Post("/upload", items);
    ASSERT_TRUE(res != nullptr) << "Upload request failed";
    EXPECT_EQ(res->status, 200) << "Upload failed with status: " << res->status;
    std::string cid = res->body;

    res = cli.Delete("/files/" + cid);
    ASSERT_TRUE(res != nullptr) << "Delete request failed";
    EXPECT_EQ(res->status, 200) << "Delete failed with status: " << res->status;
    EXPECT_EQ(res->body, "Deleted") << "Delete response body incorrect";

    res = cli.Get("/files/" + cid);
    ASSERT_TRUE(res != nullptr) << "Download after delete request failed";
    EXPECT_EQ(res->status, 404) << "Expected 404 after delete, got: " << res->status;

    removeFile(test_file_path);
}

TEST_F(IntegrationTest, DownloadNonExistent) {
    std::string fake_cid = "nonexistentcid123";
    auto res = cli.Get("/files/" + fake_cid);
    ASSERT_TRUE(res != nullptr) << "Download request failed";
    EXPECT_EQ(res->status, 404) << "Expected 404 for non-existent CID, got: " << res->status;
}

TEST_F(IntegrationTest, UpdateNonExistent) {
    std::string fake_cid = "nonexistentcid123";
    std::string test_content = "Update attempt.";
    std::string test_filename = "test_update_nonexistent.txt";
    std::string test_file_path = createTempFile(test_content, test_filename);

    httplib::MultipartFormDataItems items = {
        {"file", test_content, test_filename, "text/plain"}
    };
    auto res = cli.Put("/update/" + fake_cid, items);
    ASSERT_TRUE(res != nullptr) << "Update request failed";
    EXPECT_EQ(res->status, 404) << "Expected 404 for non-existent CID update, got: " << res->status;

    removeFile(test_file_path);
}

TEST_F(IntegrationTest, DeleteNonExistent) {
    std::string fake_cid = "nonexistentcid123";
    auto res = cli.Delete("/files/" + fake_cid);
    ASSERT_TRUE(res != nullptr) << "Delete request failed";
    EXPECT_EQ(res->status, 404) << "Expected 404 for non-existent CID delete, got: " << res->status;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}