package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The lock manager
 * which records the transactions and lock types for each page,
 * as well as the transactions that are waiting
 */
public class LockManager {
    //跟踪page上的共享锁
    private HashMap<PageId, Set<TransactionId>> sharers;
    //跟踪page上的排他锁
    private HashMap<PageId, TransactionId> owners;
    //跟踪事务上的共享锁
    private HashMap<TransactionId, Set<PageId>> sharedPages;
    //跟踪事务上的排他锁
    private HashMap<TransactionId, Set<PageId>> ownedPages;
    //跟踪page上正在等待的事务
    private HashMap<PageId, Set<TransactionId>> waiters;
    //记录每个事务正在等待哪些事务的结束
    private HashMap<TransactionId, Set<TransactionId>> waitingInfo;

    public LockManager() {
        sharers = new HashMap<PageId, Set<TransactionId>>();
        owners = new HashMap<PageId, TransactionId>();
        sharedPages = new HashMap<TransactionId, Set<PageId>>();
        ownedPages = new HashMap<TransactionId, Set<PageId>>();
        waiters = new HashMap<PageId, Set<TransactionId>>();
        waitingInfo = new HashMap<TransactionId, Set<TransactionId>>();
    }

    /**
     * A specific transaction requests to lock a specified page
     * If the permission is READ_ONLY, require a shared lock,
     * if the permission is READ_WRITE, require an exclusive lock
     * If a transaction requests a lock that it should not be granted,
     * your code should block, waiting for that lock to become available
     *
     * @param tid  the ID of the specified transaction
     * @param pid  the ID of the specified page
     * @param perm permission to determine lock type
     * @return return true if successfully acquired a lock
     * @throws TransactionAbortedException
     */
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        boolean state = false;
        if (perm.equals(Permissions.READ_WRITE)) {
            //如果Permissions对应READ_WRITE那么申请排他锁
            state = acquireExclusiveLock(tid, pid);
        } else if (perm.equals(Permissions.READ_ONLY)) {
            //如果Permissions对应READ_ONLY那么申请共享锁
            state = acquireSharedLock(tid, pid);
        } else {
            throw new DbException("");
        }
        if (state) {
            // 如果加锁申请成功
            removeWaiter(tid, pid);
            return true;
        } else {
            // 如果加锁申请失败
            addWaiter(tid, pid, perm);
            // 遍历tid依赖的事务，看它们是否间接依赖tid
            Set<TransactionId> transactionId = waitingInfo.get(tid);
            for (TransactionId tid2 : transactionId) {
                if (detectDeadLock(tid, tid2)) {
                    removeWaiter(tid, pid);
                    throw new TransactionAbortedException();
                }
            }
            return false;
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     * @return Return true if the specified transaction has a lock on the specified page
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        Set<TransactionId> sharer = sharers.get(pid);
        //如果存在tid在pid上的共享锁，返回true
        if (sharer != null && sharer.contains(tid)) {
            return true;
        }
        TransactionId owner = owners.get(pid);
        //如果存在tid在pid上的排他锁，返回true
        if (owner != null && owner.equals(tid)) {
            return true;
        }
        //如果既不存在共享锁也不存在排他锁，返回false
        return false;
    }

    /**
     * Add a shared lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */
    private void addSharer(TransactionId tid, PageId pid) {
        Set<TransactionId> sharer = sharers.get(pid);
        // 如果pid对应的共享锁序列为空
        // 那么说明pid上还没有共享锁
        if (sharer == null) {
            sharer = new HashSet<>();
        }
        sharer.add(tid);
        sharers.put(pid, sharer);
        Set<PageId> sharedPage = sharedPages.get(tid);
        // 如果事务tid还没有锁
        if (sharedPage == null) {
            sharedPage = new HashSet<>();
        }
        // tid添加一把共享锁
        sharedPage.add(pid);
        sharedPages.put(tid, sharedPage);
    }

    /**
     * Add an exclusive lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */
    private void addOwner(TransactionId tid, PageId pid) {
        owners.put(pid, tid);
        Set<PageId> ownedPage = ownedPages.get(tid);
        // 如果事务tid还没有排他锁
        if (ownedPage == null) {
            ownedPage = new HashSet<>();
        }
        // 事务tid添加一把排他锁
        ownedPage.add(pid);
        ownedPages.put(tid, ownedPage);
    }

    /**
     * Add a waitingInfo for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */

    private void addWaiter(TransactionId tid, PageId pid, Permissions perm) {
        Set<TransactionId> waiter = waiters.get(pid);
        if (waiter == null) {
            waiter = new HashSet<>();
        }
        waiter.add(tid);
        waiters.put(pid, waiter);
        Set<TransactionId> waiting = waitingInfo.get(tid);
        if (waiting == null) {
            waiting = new HashSet<>();
        }
        if (perm.equals(Permissions.READ_WRITE)) {
            Set<TransactionId> waitSharer = sharers.get(pid);
            if (waitSharer != null) {
                waiting.addAll(waitSharer);
            }
        }
        TransactionId waitOwner = owners.get(pid);
        if (waitOwner != null) {
            waiting.add(waitOwner);
        }
        waitingInfo.put(tid, waiting);
    }

    /**
     * Remove a shared lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */
    private void removeSharer(TransactionId tid, PageId pid) {
        Set<TransactionId> sharer = sharers.get(pid);
        if (sharer != null) {
            //删除pid上的tid的共享锁
            sharer.remove(tid);
            if (sharer.size() == 0) {
                //如果删除tid的共享锁之后，pid上没有共享锁了
                //在hashmap上删除这个pid
                sharers.remove(pid);
            } else {
                sharers.put(pid, sharer);
            }
        }
        Set<PageId> sharedPage = sharedPages.get(tid);
        if (sharedPage != null) {
            //删除tid上pid的共享锁
            sharedPage.remove(pid);
            if (sharedPage.size() == 0) {
                //如果删除tid对pid的共享锁之后，tid没有共享锁了
                //在hashmap上删除这个tid
                sharedPages.remove(tid);
            } else {
                sharedPages.put(tid, sharedPage);
            }
        }
    }

    /**
     * Remove an exclusive lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */
    private void removeOwner(TransactionId tid, PageId pid) {
        owners.remove(pid);
        Set<PageId> ownedPage = ownedPages.get(tid);
        if (ownedPage != null) {
            //删除tid上pid的排他锁
            ownedPage.remove(pid);
            if (ownedPage.size() == 0) {
                //如果删除tid对pid的排他锁之后，tid没有排他锁了
                //在hashmap上删除这个tid
                ownedPages.remove(tid);
            } else {
                ownedPages.put(tid, ownedPage);
            }
        }
    }

    /**
     * Remove a waitingInfo for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */
    public void removeWaiter(TransactionId tid, PageId pid) {
        Set<TransactionId> waiter = waiters.get(pid);
        if (waiter != null) {
            waiter.remove(tid);
            if (waiter.size() == 0) {
                waiters.remove(pid);
            } else {
                waiters.put(pid, waiter);
            }
        }
        // 删除tid的等待序列
        waitingInfo.remove(tid);
    }

    /**
     * Require an exclusive lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     * @return return true if successfully acquired an exclusive lock
     */
    private boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
        // page上已有的共享锁set
        Set<TransactionId> sharer = sharers.get(pid);
        // page上的排他锁
        TransactionId owner = owners.get(pid);
        if (owner != null && !owner.equals(tid)) {
            // 如果page上有不属于该事务的排他锁
            // 那么需要等待这个排他锁写完才能授予该事务排他锁
            return false;
        } else if (sharer != null && ((sharer.size() > 1) ||
                (sharer.size() == 1 && !sharer.contains(tid)))) {
            // 如果page上有共享锁：（1）有多把共享锁；（2）有一把共享锁但是不属于该事务
            // 那么需要等待读锁结束才能写锁
            return false;
        }
        if (sharer != null) {
            // 如果page上有该事务的共享锁
            // 那么可以将共享锁升级为排他锁
            // 删除pid上tid的共享锁
            removeSharer(tid, pid);
        }
        // pid上添加tid的排他锁
        addOwner(tid, pid);
        return true;
    }

    /**
     * Require a shared lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     * @return return true if successfully acquired a shared lock
     */
    private boolean acquireSharedLock(TransactionId tid, PageId pid) {
        TransactionId owner = owners.get(pid);
        // 如果page上有排他锁并且排他锁不属于该事务，那么不能申请共享锁
        if (owner != null && !owner.equals(tid)) {
            return false;
        } else if (owner == null) {
            //如果没有排他锁，那么共享锁申请成功
            addSharer(tid, pid);
        }
        return true;
    }

    /**
     * Releases the lock on a page.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        Set<PageId> sharePage = sharedPages.get(tid);
        if (sharePage != null && sharePage.contains(pid)) {
            removeSharer(tid, pid);
        }
        Set<PageId> ownPage = ownedPages.get(tid);
        if (ownPage != null && ownPage.contains(pid)) {
            removeOwner(tid, pid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        if (sharedPages.get(tid) != null) {
            for (PageId pid : sharedPages.get(tid)) {
                // 遍历所有有tid的共享锁的page
                // 然后将page上tid的共享锁remove
                // 这里不能调用removeSharer(),否则回修改sharedPages抛出异常
                Set<TransactionId> sharer = sharers.get(pid);
                sharer.remove(tid);
                if (sharer.size() == 0) {
                    sharers.remove(pid);
                } else {
                    sharers.put(pid, sharer);
                }
            }
            // 删除tid的所有共享锁后，最后删除tid即可
            sharedPages.remove(tid);
        }
        if (ownedPages.get(tid) != null) {
            for (PageId pid : ownedPages.get(tid)) {
                owners.remove(pid);
            }
            ownedPages.remove(tid);
        }
    }

    /**
     * detect if there a deadlock,
     * example:
     * t1->p1.r | t2->p2.r | t3->p3.r
     * waiter: t2->p1.w | t3->p2.w | t1->p3.w
     * waitingInfo: t1->t3 | t3->t2 | t2->t1
     * now there a deadlock, t1 are waiting for p3 which was locked by t3,
     * and t3 is indirectly waiting for t1 to release p1
     * If tn in t1's waitingList and t1 in tn's waitingList, there may be a deadlock
     *
     * @param tid1 the ID of the transaction that failed to acquire a lock
     * @param tid2 the ID of the transaction that directly or indirectly block tid1
     * @return return true if there is a deadlock
     */
    public synchronized boolean detectDeadLock(TransactionId tid1, TransactionId tid2) {
        if (tid1.equals(tid2)) {
            // 如果tid2和tid1相等，说明tid1间接依赖本身的结束
            return true;
        } else {
            Set<TransactionId> tid = waitingInfo.get(tid2);
            if (tid == null) {
                return false;
            } else {
                // 递归
                // 遍历tid2的所有等待的事务，如果它们间接等待tid1则说明发生了死锁
                for (TransactionId t : tid) {
                    if (detectDeadLock(tid1, t)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}