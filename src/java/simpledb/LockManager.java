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
    private HashMap<PageId, List<TransactionId>> sharers;
    private HashMap<PageId, TransactionId> owners;
    private HashMap<TransactionId, List<PageId>> sharedPages;
    private HashMap<TransactionId, List<PageId>> ownedPages;
    private HashMap<PageId, List<TransactionId>> waiters;
    private HashMap<TransactionId, List<PageId>> waitedPages;

    public LockManager() {
        sharers = new HashMap<PageId, List<TransactionId>>();
        owners = new HashMap<PageId, TransactionId>();
        sharedPages = new HashMap<TransactionId, List<PageId>>();
        ownedPages = new HashMap<TransactionId, List<PageId>>();
        waiters = new HashMap<PageId, List<TransactionId>>();
        waitedPages = new HashMap<TransactionId, List<PageId>>();
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
            addWaiter(tid, pid);
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
        List<TransactionId> sharer = sharers.get(pid);
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
        List<TransactionId> sharer = sharers.get(pid);
        if (sharer == null) {
            sharer = new ArrayList<>();
        }
        sharer.add(tid);
        sharers.put(pid, sharer);
        List<PageId> sharedPage = sharedPages.get(tid);
        if (sharedPage == null) {
            sharedPage = new ArrayList<>();
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
        List<PageId> ownedPage = ownedPages.get(tid);
        if (ownedPage == null) {
            ownedPage = new ArrayList<>();
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
    private void addWaiter(TransactionId tid, PageId pid) {
        List<TransactionId> waiter = waiters.get(pid);
        if (waiter == null) {
            waiter = new ArrayList<>();
        }
        waiter.add(tid);
        waiters.put(pid, waiter);
        List<PageId> waitedPage = waitedPages.get(tid);
        if (waitedPage == null) {
            waitedPage = new ArrayList<>();
        }
        waitedPage.add(pid);
        waitedPages.put(tid, waitedPage);
    }

    /**
     * Remove a shared lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */
    private void removeSharer(TransactionId tid, PageId pid) {
        List<TransactionId> sharer = sharers.get(pid);
        sharer.remove(tid);
        if (sharer.size() == 0) {
            sharers.remove(pid);
        } else {
            sharers.put(pid, sharer);
        }
        List<PageId> sharedPage = sharedPages.get(tid);
        sharedPage.remove(pid);
        if (sharedPage.size() == 0) {
            sharedPages.remove(tid);
        } else {
            sharedPages.put(tid, sharedPage);
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
        List<PageId> ownedPage = ownedPages.get(tid);
        ownedPage.remove(pid);
        if (ownedPage.size() == 0) {
            ownedPages.remove(tid);
        } else {
            ownedPages.put(tid, ownedPage);
        }
    }

    /**
     * Remove a waitingInfo for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     */
    private void removeWaiter(TransactionId tid, PageId pid) {
        List<TransactionId> waiter = waiters.get(pid);
        if (waiter == null) {
            return;
        }
        waiter.remove(tid);
        if (waiter.size() == 0) {
            waiters.remove(pid);
        } else {
            waiters.put(pid, waiter);
        }
        List<PageId> waitedPage = waitedPages.get(tid);
        waitedPage.remove(pid);
        if (waitedPage.size() == 0) {
            waitedPages.remove(tid);
        } else {
            waitedPages.put(tid, waitedPage);
        }
    }

    /**
     * Require an exclusive lock for the specified transaction on the specified page
     *
     * @param tid the ID of the specified transaction
     * @param pid the ID of the specified page
     * @return return true if successfully acquired an exclusive lock
     */
    private boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
        List<TransactionId> sharer = sharers.get(pid);
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
        List<PageId> sharePage = sharedPages.get(tid);
        if (sharePage != null && sharePage.contains(pid)) {
            removeSharer(tid, pid);
        }
        List<PageId> ownPage = ownedPages.get(tid);
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
                List<TransactionId> sharer = sharers.get(pid);
                sharer.remove(tid);
                if (sharer.size() == 0) sharers.remove(pid);
                else sharers.put(pid, sharer);
            }
            sharedPages.remove(tid);
        }
        if (ownedPages.get(tid) != null) {
            for (PageId pid : ownedPages.get(tid)) {
                owners.remove(pid);
            }
            ownedPages.remove(tid);
        }
        /*
        if (waitedPages.get(tid) != null) {
            for (PageId pid : waitedPages.get(tid)) {
                List<TransactionId> waiter = waiters.get(pid);
                waiter.remove(tid);
                if (waiter.size() == 0) waiters.remove(pid);
                else waiters.put(pid, waiter);
            }
            waitedPages.remove(tid);
        }
         */
    }
}