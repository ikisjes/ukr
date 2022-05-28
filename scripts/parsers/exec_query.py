from query import Query
import sys

if __name__ == '__main__':
    args = sys.argv[1:]
    meth = args[0]
    q=Query()
    if not hasattr(q, meth):
        print("No such method- %s!" % meth)
        sys.exit()
    else:
        getattr(q, meth)(*args[1:])
    #q.makeNetwork()
