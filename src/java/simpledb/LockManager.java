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
    private HashMap<PageId, Set<TransactionId>> sharers;
    private HashMap<PageId, TransactionId> owners;
    private HashMap<TransactionId, Set<PageId>> sharedPages;
    private HashMap<TransactionId, Set<PageId>> ownedPages;
    private HashMap<PageId, Set<TransactionId>> waiters;
    private HashMap<TransactionId, Set<PageId>> waitedPages;
    //记录每个事务正在等待哪些事务的结束
    private HashMap<TransactionId, List<TransactionId>> waitingInfo;

    public LockManager() {
        sharers = new HashMap<PageId, Set<TransactionId>>();
        owners = new HashMap<PageId, TransactionId>();
        sharedPages = new HashMap<TransactionId, Set<PageId>>();
        ownedPages = new HashMap<TransactionId, Set<PageId>>();
        waiters = new HashMap<PageId, Set<TransactionId>>();
        waitedPages = new HashMap<TransactionId, Set<PageId>>();
        waitingInfo = new HashMap<TransactionId, List<TransactionId>>();
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
            throws TransactionAbortedException {
        boolean state = false;
        if (perm.equals(Permissions.READ_WRITE)) {
            state = acquireExclusiveLock(tid, pid);
        } else if (perm.equals(Permissions.READ_ONLY)) {
            state = acquireSharedLock(tid, pid);
        } else {
            throw new TransactionAbortedException();
        }
        if (state) {
            removeWaiter(tid, pid);
            return true;
        } else {
            addWaiter(tid, pid, perm);
            List<TransactionId> transactionId = waitingInfo.get(tid);
            for (TransactionId tid2 : transactionId) {
                if (detectDeadLock(tid, tid2)) {
                    waitingInfo.remove(tid);
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
        if (sharer != null && sharer.contains(tid)) {
            return true;
        }
        TransactionId owner = owners.get(pid);
        if (owner != null && owner.equals(tid)) {
            return true;
        }
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
        if (sharer == null) {
            sharer = new HashSet<>();
        }
        sharer.add(tid);
        sharers.put(pid, sharer);
        Set<PageId> sharedPage = sharedPages.get(tid);
        if (sharedPage == null) {
            sharedPage = new HashSet<>();
        }
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
        if (ownedPage == null) {
            ownedPage = new HashSet<>();
        }
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
        Set<PageId> waitedPage = waitedPages.get(tid);
        if (waitedPage == null) {
            waitedPage = new HashSet<>();
        }
        waitedPage.add(pid);
        waitedPages.put(tid, waitedPage);
        List<TransactionId> waiting = waitingInfo.get(tid);
        if (waiting == null) {
            waiting = new ArrayList<>();
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
            sharer.remove(tid);
            if (sharer.size() == 0) {
                sharers.remove(pid);
            } else {
                sharers.put(pid, sharer);
            }
        }
        Set<PageId> sharedPage = sharedPages.get(tid);
        if (sharedPage != null) {
            sharedPage.remove(pid);
            if (sharedPage.size() == 0) {
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
            ownedPage.remove(pid);
            if (ownedPage.size() == 0) {
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
    private void removeWaiter(TransactionId tid, PageId pid) {
        Set<TransactionId> waiter = waiters.get(pid);
        if (waiter != null) {
            waiter.remove(tid);
            if (waiter.size() == 0) {
                waiters.remove(pid);
            } else {
                waiters.put(pid, waiter);
            }
        }
        Set<PageId> waitedPage = waitedPages.get(tid);
        if (waitedPage != null) {
            waitedPage.remove(pid);
            if (waitedPage.size() == 0) {
                waitedPages.remove(tid);
            } else {
                waitedPages.put(tid, waitedPage);
            }
        }
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
        Set<TransactionId> sharer = sharers.get(pid);
        TransactionId owner = owners.get(pid);
        if (owner != null && !owner.equals(tid)) {
            return false;
        } else if (sharer != null && ((sharer.size() > 1) || (sharer.size() == 1 && !sharer.contains(tid)))) {
            return false;
        }
        if (sharer != null) {
            removeSharer(tid, pid);
        }
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
        if (owner != null && !owner.equals(tid)) {
            return false;
        } else if (owner == null) {
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
                Set<TransactionId> sharer = sharers.get(pid);
                sharer.remove(tid);
                if (sharer.size() == 0) {
                    sharers.remove(pid);
                } else {
                    sharers.put(pid, sharer);
                }
            }
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
            return true;
        } else {
            List<TransactionId> tid = waitingInfo.get(tid2);
            if (tid == null) {
                return false;
            } else {
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