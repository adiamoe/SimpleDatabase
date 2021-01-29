package simpledb;

public class LockState {
    private final TransactionId tid;
    private final Permissions perm;

    public LockState(TransactionId tid, Permissions perm)
    {
        this.tid = tid;
        this.perm = perm;
    }

    public TransactionId getId()
    {
        return tid;
    }

    public Permissions getPerm()
    {
        return perm;
    }

    public boolean equals(Object o)
    {
        if(this == o)
            return true;
        if(o instanceof LockState)
        {
            LockState ls = (LockState) o;
            if(ls.tid.equals(tid))
            {
                return ls.perm == perm;
            }
        }
        return false;
    }


}
