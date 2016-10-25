//
// Created by harvey on 2016/9/14.
//

#ifndef DBF_READER_LOCK_H
#define DBF_READER_LOCK_H
#ifdef WIN32
#include <Windows.h>

#else
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#endif

#include <fcntl.h>


class xLock {
private:
#ifdef WIN32
	CRITICAL_SECTION mSection;
#else
	pthread_mutex_t     mMutex;
#endif

public:
	inline xLock() {
#ifdef WIN32
		InitializeCriticalSection(&mSection);
#else
		pthread_mutexattr_t attr;
		pthread_mutexattr_init(&attr);
		pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
		int ret = pthread_mutex_init(&mMutex, &attr);
		if (ret != 0) {
			fprintf(stderr, "pthread_mutex_init error %d \n\r", ret);
		}
#endif
	};

	inline ~xLock() {
#ifdef WIN32
		DeleteCriticalSection(&mSection);
#else
		pthread_mutex_destroy(&mMutex);
#endif
	}

	inline void Enter() {
#ifdef WIN32
		EnterCriticalSection(&mSection);
#else
		pthread_mutex_lock(&mMutex);
#endif
	}

	inline void Leave() {
#ifdef WIN32
		LeaveCriticalSection(&mSection);
#else
		pthread_mutex_unlock(&mMutex);
#endif
	};
};

class CLockUser {
public:
	inline CLockUser(xLock& lock) : mlock(lock) {
		mlock.Enter();
	};

	inline ~CLockUser() {
		mlock.Leave();
	}

private:
	xLock& mlock;
};

#define XLOCK(T) CLockUser lock(T)

class LockFileGuard {
public:
	LockFileGuard() 
		: hFile_(INVALID_HANDLE_VALUE){
		
	}

	bool CheckLock(std::string& filename) {
		filename_ = filename;
		size_t size = filename_.length();
		buffer_ = new wchar_t[size + 1];
		MultiByteToWideChar(CP_ACP, 0, filename_.c_str(), size, buffer_, size * sizeof(wchar_t));
		buffer_[size] = '\0';
		hFile_ = CreateFile(buffer_,
			0,			/*TUDO GENERIC_READ*/ 
			FILE_SHARE_READ,
			NULL,
			OPEN_EXISTING,
			FILE_ATTRIBUTE_READONLY,  //FILE_FLAG_SEQUENTIAL_SCAN 针对连续访问对文件缓冲进行优化
			NULL);
		if (INVALID_HANDLE_VALUE == hFile_) {
			return false;
		}

		LockFileEx(hFile_,
			LOCKFILE_EXCLUSIVE_LOCK,
			0,
			MAXDWORD,
			MAXDWORD,
			&overlapvar_);
		return true;
	}

	~LockFileGuard() {
		UnlockFileEx(hFile_, 0, MAXDWORD, MAXDWORD, &overlapvar_);
		CloseHandle(hFile_);
		if (NULL != buffer_) {
			delete[] buffer_;
			buffer_ = NULL;
		}
	}

private:
	wchar_t* buffer_;
	std::string filename_;
	OVERLAPPED overlapvar_ = {0};
	HANDLE hFile_;
};

#endif //DBF_READER_LOCK_H
