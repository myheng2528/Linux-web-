#include "epoller.h"

Epoller::Epoller(int maxEvent) 
    : epollFd_(epoll_create1(0)), events_(maxEvent) // 使用 epoll_create1
{
    assert(epollFd_ >= 0); // 确保 epollFd_ 创建成功
    assert(maxEvent > 0); // 确保 maxEvent 是合理的 
    assert(events_.size() > 0); // 确保 events_ 大小合理
}
Epoller::~Epoller()
{
    close(epollFd_);
}

bool Epoller::AddFd(int fd, uint32_t events)
{
    if (fd < 0)
        return false;
    epoll_event ev = {0};
    ev.data.fd = fd;
    ev.events = events;
    return 0 == epoll_ctl(epollFd_, EPOLL_CTL_ADD, fd, &ev);
}

bool Epoller::ModFd(int fd, uint32_t events)
{
    if (fd < 0)
        return false;
    epoll_event ev = {0};
    ev.data.fd = fd;
    ev.events = events;
    return 0 == epoll_ctl(epollFd_, EPOLL_CTL_MOD, fd, &ev);
}

bool Epoller::DelFd(int fd)
{
    if (fd < 0)
        return false;
    return 0 == epoll_ctl(epollFd_, EPOLL_CTL_DEL, fd, 0);
}

// 返回事件数量
int Epoller::Wait(int timeoutMs)
{
    return epoll_wait(epollFd_, &events_[0], static_cast<int>(events_.size()), timeoutMs);
}

// 获取事件的fd
int Epoller::GetEventFd(size_t i) const
{
    assert(i < events_.size() && i >= 0);
    return events_[i].data.fd;
}

// 获取事件属性
uint32_t Epoller::GetEvents(size_t i) const
{
    assert(i < events_.size() && i >= 0);
    return events_[i].events;
}