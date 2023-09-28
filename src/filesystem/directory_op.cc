#include <algorithm>
#include <sstream>
#include "fmt/core.h"
#include "filesystem/directory_op.h"

namespace chfs
{

    auto type_to_str(InodeType type) -> std::string
    {
        switch (type) {
            case InodeType::Unknown:
                return "unknown";
            case InodeType::FILE:
                return "file";
            case InodeType::Directory:
                return "directory";
        }
    }

/**
 * Some helper functions
 */
    auto string_to_inode_id(std::string &data) -> inode_id_t
    {
        std::stringstream ss(data);
        inode_id_t inode;
        ss >> inode;
        return inode;
    }

    auto inode_id_to_string(inode_id_t id) -> std::string
    {
        std::stringstream ss;
        ss << id;
        return ss.str();
    }

// {Your code here}
    auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string
    {
        std::ostringstream oss;
        usize cnt = 0;
        for (const auto &entry: entries) {
            oss << entry.name << ':' << entry.id;
            if (cnt < entries.size() - 1) {
                oss << '/';
            }
            cnt += 1;
        }
        return oss.str();
    }

// {Your code here}
    auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string
    {
        //       Append the new directory entry to `src`.
        std::string res;
        if (src.empty()) {
            res = fmt::format("{}:{}", filename, inode_id_to_string(id));
        } else { res = fmt::format("{}/{}:{}", src, filename, inode_id_to_string(id)); }
        return res;
    }

// {Your code here}
    void parse_directory(std::string &src, std::list<DirectoryEntry> &list)
    {
        size_t it1 = 0, it2 = 0;
        std::string filename{}, id_str{};
        while (true) {
            auto temp1 = src.find(':', it1);
            auto temp2 = src.find('/', it2);
            if (temp1 == -1 && temp2 == -1) {
                return;
            }
            if (temp2 == -1) {
                filename = src.substr(it2, temp1 - it2);
                id_str = src.substr(temp1 + 1, src.size() - temp1);
                list.emplace_back(DirectoryEntry{filename, string_to_inode_id(id_str)});
                return;
            }
            filename = src.substr(it2, temp1 - it2);
            id_str = src.substr(temp1 + 1, temp2 - temp1 - 1);
            it1 = temp1 + 1;
            it2 = temp2 + 1;
            list.emplace_back(DirectoryEntry{filename, string_to_inode_id(id_str)});
        }

    }

// {Your code here}
    auto rm_from_directory(std::string src, std::string filename) -> std::string
    {
        auto it = src.find(filename);
        if (it == -1) {
            return src;
        }
        auto it2 = src.find('/', it);
        if (it2 == -1) {
            if (it == 0) {
                src.erase(it);
            } else {
                src.erase(it - 1);
            }
        } else {
            if (it == 0) {
                src.erase(it, it2 - it + 1);
            } else {
                src.erase(it - 1, it2 - it + 1);
            }
        }
        //       Remove the directory entry from `src`.
        return src;
    }

/**
 * { Your implementation here }
 */
    auto read_directory(FileOperation *fs, inode_id_t id,
                        std::list<DirectoryEntry> &list) -> ChfsNullResult
    {
        auto read_res = fs->read_file(id);
        if (read_res.is_err()) {
            return ChfsNullResult{read_res.unwrap_error()};
        }
        auto buffer = read_res.unwrap();
        auto src = std::string(buffer.begin(), buffer.end());

        parse_directory(src, list);
        return KNullOk;
    }

// {Your code here}
    auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t>
    {
        std::list<DirectoryEntry> list;
        read_directory(this, id, list);
        for (const auto &de: list) {
            if (strcmp(de.name.c_str(), name) == 0) {
                return ChfsResult<inode_id_t>{de.id};
            }
        }
        return ChfsResult<inode_id_t>(ErrorType::NotExist);
    }

// {Your code here}
    auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t>
    {
        auto lookup_res = lookup(id, name);
        if (lookup_res.is_ok()) {
            return ChfsResult<inode_id_t>{ErrorType::AlreadyExist};
        }
        auto block_id_res = this->block_allocator_->allocate();
        if (block_id_res.is_err()) {
            return ChfsResult<inode_id_t>{block_id_res.unwrap_error()};
        }
        auto block_id = block_id_res.unwrap();
        auto inode_id_res = this->inode_manager_->allocate_inode(type, block_id);
        if (inode_id_res.is_err()) {
            return ChfsResult<inode_id_t>{inode_id_res.unwrap_error()};
        }
        auto inode_id = inode_id_res.unwrap();
        auto read_res = this->read_file(id);
        if (read_res.is_err()) {
            return ChfsResult<inode_id_t>{read_res.unwrap_error()};
        }
        auto buffer = read_res.unwrap();
        auto src = std::string(buffer.begin(), buffer.end());
        auto res = append_to_directory(src, std::string{name}, inode_id);
        buffer.clear();
        buffer = std::vector<uint8_t>{res.begin(), res.end()};
        this->write_file(id, buffer);
        // 1. Check if `name` already exists in the parent.
        //    If already exist, return ErrorType::AlreadyExist.
        // 2. Create the new inode.
        // 3. Append the new entry to the parent directory.

        return ChfsResult<inode_id_t>{inode_id};
    }

// {Your code here}
    auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult
    {
        auto read_res = this->read_file(parent);
        if (read_res.is_err()) {
            return ChfsNullResult{read_res.unwrap_error()};
        }
        auto buffer = read_res.unwrap();
        auto src = std::string(buffer.begin(), buffer.end());
        auto res = rm_from_directory(src, std::string{name});
        buffer.clear();
        buffer = std::vector<uint8_t>{res.begin(), res.end()};
        this->write_file(parent, buffer);
        // 1. Remove the file, you can use the function `remove_file`
        // 2. Remove the entry from the directory.

        return KNullOk;
    }

} // namespace chfs
