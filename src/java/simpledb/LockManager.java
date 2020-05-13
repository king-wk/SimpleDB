package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 *
 */
public class LockManager {
    //页面对应有共享锁的事务
    private HashMap<PageId, List<TransactionId>> sharers;
    //页面对应排他锁的事务
    private HashMap<PageId, TransactionId> owners;
    //事务对应有共享锁的page
    //private HashMap<TransactionId, List<PageId>> sharePages;
    //事务对应有排他锁的page
    //private HashMap<TransactionId, List<PageId>> ownPages;
    private HashMap<PageId, List<TransactionId>> waiters;
    //private HashMap<TransactionId,PageId>waitPages;
    //private HashMap<PageId,TransactionId>waitingInfo;


    public LockManager() {
        sharers = new HashMap<PageId, List<TransactionId>>();
        owners = new HashMap<PageId, TransactionId>();
        //sharePages = new HashMap<TransactionId, List<PageId>>();
        //ownPages = new HashMap<TransactionId, List<PageId>>();
        waiters = new HashMap<PageId, List<TransactionId>>();
        //waitPages=new HashMap<TransactionId, PageId>();
        //waitingInfo=new HashMap<PageId, TransactionId>();
    }

    /**
     * @param pageId
     * @param transactionId
     * @return
     */
    private boolean acquireXLock(PageId pageId, TransactionId transactionId) {
        List<TransactionId> sharer = sharers.get(pageId);
        TransactionId owner = owners.get(pageId);
        //如果这个page已经有了一个排他锁，而且不是这个事务加的排他锁，那么申请失败
        if (owner != null && !owner.equals(transactionId)) {
            return false;
        }
        //如果这个page已经有多个或者一个但是不是该事务加的共享锁，必须等这些共享锁释放才能加排他锁，申请失败
        if (sharer != null && (sharer.size() > 1 || (sharer.size() == 1 && !sharer.contains(transactionId)))) {
            return false;
        }
        //该事务是这个page上有共享锁的唯一事务
        //将共享锁升级为排他锁
        if (sharer != null) {
            removeSharer(pageId, transactionId);
        }
        addOwner(pageId, transactionId);
        return true;
    }

    /**
     * @param pageId
     * @param transactionId
     * @return
     */
    private boolean acquireSLock(PageId pageId, TransactionId transactionId) {
        TransactionId owner = owners.get(pageId);
        if (owner != null && !owner.equals(transactionId)) {
            return false;
        }
        if (owner == null) {
            addSharer(pageId, transactionId);
        }
        return true;
    }

    /**
     * @param pageId
     * @param transactionId
     */
    private void addSharer(PageId pageId, TransactionId transactionId) {
        List<TransactionId> sharer = sharers.get(pageId);
        if (sharer == null) {
            sharer = new ArrayList<TransactionId>();
        }
        sharer.add(transactionId);
        sharers.put(pageId, sharer);
        //List<PageId> sharePage = sharePages.get(transactionId);
        //if (sharePage == null) {
        //    sharePage = new ArrayList<PageId>();
        //}
        //sharePage.add(pageId);
        //sharePages.put(transactionId, sharePage);
    }

    /**
     * @param pageId
     * @param transactionId
     */
    private void removeSharer(PageId pageId, TransactionId transactionId) {
        List<TransactionId> sharer = sharers.get(pageId);
        sharer.remove(transactionId);
        if (sharer.size() == 0) {
            sharers.remove(pageId);
        } else {
            sharers.put(pageId, sharer);
        }
        //List<PageId> sharePage = sharePages.get(transactionId);
        //sharePage.remove(pageId);
        //if (sharePage.size() == 0) {
        //    sharePages.remove(transactionId);
        //} else {
        //    sharePages.put(transactionId, sharePage);
        //}
    }

    /**
     * @param pageId
     * @param transactionId
     */
    private void addOwner(PageId pageId, TransactionId transactionId) {
        owners.put(pageId, transactionId);
        //List<PageId> ownPage = ownPages.get(transactionId);
        //if (ownPage == null) {
        //    ownPage = new ArrayList<PageId>();
        //}
        //ownPage.add(pageId);
        //ownPages.put(transactionId, ownPage);
    }

    /**
     * @param pageId
     * @param transactionId
     */
    private void removeOwner(PageId pageId, TransactionId transactionId) {
        owners.remove(pageId);
    }


    /**
     * @param pageId
     * @param transactionId
     * @param permissions
     * @return
     */
    public synchronized boolean Lock(PageId pageId, TransactionId transactionId, Permissions permissions) throws TransactionAbortedException {
        boolean state = false;
        if (Permissions.READ_WRITE.equals(permissions)) {
            state = acquireXLock(pageId, transactionId);
        } else if (Permissions.READ_ONLY.equals(permissions)) {
            state = acquireSLock(pageId, transactionId);
        } else {
            throw new TransactionAbortedException();
        }
        if (state) {
            if (waiters.containsKey(pageId)) {
                List<TransactionId> waiter = waiters.get(pageId);
                if (waiter.contains(transactionId)) {
                    waiter.remove(transactionId);
                }
                if (waiter.size() == 0) {
                    waiters.remove(pageId);
                } else {
                    waiters.put(pageId, waiter);
                }
            }
            return true;
        } else {
            if (!waiters.containsKey(pageId)) {
                waiters.put(pageId, new ArrayList<TransactionId>());
            }
            List<TransactionId> waiter = waiters.get(pageId);
            if (!waiter.contains(transactionId)) {
                waiter.add(transactionId);
            }
            waiters.put(pageId, waiter);
        }
        return false;
    }

    /**
     * @param transactionId the ID of the transaction requesting the unlock
     * @param pageId        the ID of the page to unlock
     */
    public synchronized void releaseLock(TransactionId transactionId, PageId pageId) {
        if (sharers.containsKey(pageId)) {
            List<TransactionId> list = sharers.get(pageId);
            if (list.contains(transactionId)) {
                removeSharer(pageId, transactionId);
            }
        }
        if (owners.get(pageId).equals(transactionId)) {
            removeOwner(pageId, transactionId);
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     *
     * @param transactionId
     * @param pageId
     * @return
     */
    public synchronized boolean holdsLock(TransactionId transactionId, PageId pageId) {
        if (sharers.get(pageId).contains(transactionId) || owners.get(pageId).equals(transactionId)) {
            return true;
        }
        return false;
    }
}
