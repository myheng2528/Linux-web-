#include "sqlconnpool.h"
#include <condition_variable>
#include <mutex>
#include <queue>

SqlConnPool* SqlConnPool::Instance() {
    static SqlConnPool pool;
    return &pool;
}

// 初始化
void SqlConnPool::Init(const char* host, uint16_t port,
              const char* user, const char* pwd, 
              const char* dbName, int connSize) {
    assert(connSize > 0);
    for (int i = 0; i < connSize; i++) {
        MYSQL* conn = mysql_init(nullptr);
        if (!conn) {
            LOG_ERROR("MySql init error!");
            assert(conn);
        }
        conn = mysql_real_connect(conn, host, user, pwd, dbName, port, nullptr, 0);
        if (!conn) {
            LOG_ERROR("MySql Connect error!");
        }
        connQue_.emplace(conn);
    }
    MAX_CONN_ = connSize;
}

MYSQL* SqlConnPool::GetConn() {
    std::unique_lock<std::mutex> locker(mtx_);
    while (connQue_.empty()) {  // 使用 while 循环来处理虚假唤醒
        LOG_WARN("SqlConnPool busy, waiting for connection!");
        cv_.wait(locker);  // 等待连接可用
    }
    MYSQL* conn = connQue_.front();
    connQue_.pop();
    return conn;
}

// 存入连接池，实际上没有关闭
void SqlConnPool::FreeConn(MYSQL* conn) {
    assert(conn);
    {
        std::lock_guard<std::mutex> locker(mtx_);
        connQue_.push(conn);
    }
    cv_.notify_one();  // 通知一个等待线程
}

void SqlConnPool::ClosePool() {
    std::lock_guard<std::mutex> locker(mtx_);
    while (!connQue_.empty()) {
        auto conn = connQue_.front();
        connQue_.pop();
        mysql_close(conn);
    }
    mysql_library_end();
}

int SqlConnPool::GetFreeConnCount() {
    std::lock_guard<std::mutex> locker(mtx_);
    return connQue_.size();
}
