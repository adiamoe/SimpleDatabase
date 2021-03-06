package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//给每个bufferpool添加一个LockManager
public class LockManager {

    //每个page上锁的状态
    private final Map<PageId, ArrayList<LockState>> lockOnPage;
    //事务在等待的page
    private final Map<TransactionId, PageId> waitForLock;

    public LockManager()
    {
        lockOnPage = new ConcurrentHashMap<>();
        waitForLock = new ConcurrentHashMap<>();
    }

    public synchronized boolean requireLock(TransactionId tid, PageId pid, Permissions perm)
    {
        return perm.permLevel == 1?requireXLock(tid, pid):requireSLock(tid, pid);
    }

    private synchronized boolean requireSLock(TransactionId tid, PageId pid)
    {
        ArrayList<LockState> ls = lockOnPage.get(pid);
        //如果没有锁，直接加锁
        if(ls == null)
            return lock(tid, pid, Permissions.READ_ONLY);
        int numLock = ls.size();
        //有一个锁可能有
        //1.自己的读锁 2. 自己的写锁 3. 别人的读锁 4.别人的写锁
        if(numLock == 0)
            return lock(tid, pid, Permissions.READ_ONLY);
        else if(numLock == 1)
        {
            LockState s = ls.iterator().next();
            if(s.getId().equals(tid) && s.getPerm() == Permissions.READ_ONLY)
                return true;
            else if(s.getId().equals(tid) && s.getPerm() == Permissions.READ_WRITE
                    ||s.getPerm() == Permissions.READ_ONLY)
                return lock(tid, pid, Permissions.READ_ONLY);
            else
            {
                waitForLock.put(tid, pid);
                return false;
            }
        }
        //有多个锁
        //1.多个事务的读锁 2.一个事务的读锁和写锁
        else
        {
            for(LockState s: ls)
            {
                //如果有其他事务的写锁，获取失败，等待
                if(!s.getId().equals(tid) && s.getPerm() == Permissions.READ_WRITE)
                {
                    waitForLock.put(tid, pid);
                    return false;
                }
                //如果有自己的锁，如果是读锁，返回；如果是写锁，说明其他锁也一定是自己的读锁，同样返回
                if(s.getId().equals(tid))
                {
                    return true;
                }
            }
            return lock(tid, pid, Permissions.READ_ONLY);
        }
    }

    private synchronized boolean requireXLock(TransactionId tid, PageId pid)
    {
        ArrayList<LockState> ls = lockOnPage.get(pid);
        if(ls == null)
            return lock(tid, pid, Permissions.READ_WRITE);
        int numLock = ls.size();
        if(numLock == 0)
            return lock(tid, pid, Permissions.READ_WRITE);
        else if(numLock == 1)
        {
            //如果是自己的写锁，返回；如果是自己的读锁，加上写锁后返回(Upgrade)，不是自己的锁则获取失败
            LockState s = ls.iterator().next();
            if(s.getId().equals(tid))
                return s.getPerm() == Permissions.READ_WRITE || lock(tid, pid, Permissions.READ_WRITE);
            waitForLock.put(tid, pid);
            return false;
        }
        else
        {
            for(LockState s:ls)
            {
                //如果有自己的写锁，返回
                if(s.getId().equals(tid) && s.getPerm() == Permissions.READ_WRITE)
                    return true;
            }
            //说明有其他事务的锁，获取失败
            waitForLock.put(tid, pid);
            return false;
        }
    }

    private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm)
    {
        LockState s = new LockState(tid, perm);
        ArrayList<LockState> list = lockOnPage.get(pid);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(s);
        lockOnPage.put(pid, list);
        waitForLock.remove(tid);
        return true;
    }

    public synchronized boolean unlock(TransactionId tid, PageId pid)
    {
        ArrayList<LockState> list = lockOnPage.get(pid);
        if(list == null)
            return true;
        boolean flag = list.removeIf(s -> s.getId().equals(tid));
        if(flag)
        {
            lockOnPage.put(pid, list);
            return true;
        }
        return false;
    }

    public synchronized void releaseAllLock(TransactionId tid)
    {
        ArrayList<PageId> toRelease = getAllLocksByTid(tid);
        for (PageId pid : toRelease) {
            unlock(tid, pid);
        }
    }

    //说明有事务在间接等待自己锁住的page
    //从被锁住的page出发，检索拥有该page的事务
    public synchronized boolean detectDeadLock(TransactionId tid, PageId pid)
    {
        ArrayList<LockState> ls = lockOnPage.get(pid);
        if(ls == null||ls.size() == 0)
            return false;
        for(LockState s:ls)
        {
            if(!s.getId().equals(tid))
            {
                boolean waitting = waitForPage(tid, s.getId());
                if(waitting)
                    return true;
            }
        }
        return false;
    }

    //查询事务所等待的page，再从这些page出发重新搜索
    private synchronized boolean waitForPage(TransactionId end, TransactionId node)
    {
        ArrayList<PageId> locks = getAllLocksByTid(end);
        PageId waitfor = waitForLock.get(node);
        if(waitfor == null)
            return false;
        if(locks.contains(waitfor)) {
            return true;
        }
        return detectDeadLock(end, waitfor);
    }

    public boolean islock(TransactionId tid, PageId pid)
    {
        ArrayList<PageId> locks = getAllLocksByTid(tid);
        return locks.contains(pid);
    }

    private synchronized ArrayList<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> pids = new ArrayList<>();
        for (Map.Entry<PageId, ArrayList<LockState>> entry : lockOnPage.entrySet()) {
            for (LockState ls : entry.getValue()) {
                if (ls.getId().equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
        return pids;
    }

}
