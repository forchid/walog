# walog
A simple and high performance standalone, client/server or master/slave replication WAL implementation, 
supports multi-thread or multi-process iterating/appending log.

- Single thread appends 200,000+ items per second, iterates 1000,000+ items per second
- Support multi-processes iterate/append logs
- Simple API such as append(log)/first()/get(lsn)/next(log)/iterator()/iterator(lsn)/sync() etc
- Require JDK 7+
- Support simple API of wal client/server arch
- Provide wal master/slave replication framework, and in-process/rmi implementations

## examples
- Open wal logger
```java
    // Open a standalone wal logger
    import org.walog.rmi.RmiWalServer;
    import org.walog.*;
    
    public class Test {
        public static void main(String[] args) {
            File dir = new File("./");
            try(Waler waler = WalerFactory.open(dir)){
                // iterate/append operations
            }
        }
    }
    
    // Open a master/slave wal logger
    public class Test {
            public static void main(String[] args) {
                // Boot a master server
                String[] conf = {"-d", "./master"};
                RmiWalServer.start(conf);
                
                // Connect to the master and replicate it
                File dir = new File("./");
                String url = "walog:rmi:slave://localhost/wal?dataDir=" + dir;
                try(SlaveWaler slaveWaler = WalDriverManager.connect(url)){
                    // iterate/append operations on master wal logger
                    Waler master = slaveWaler.getMaster();
                    // iterate/fetch operations on slave wal logger
                }
            }
    }
```
- Append wal
```java
    import org.walog.Wal;
    import org.walog.Waler;
    import org.walog.WalerFactory;

    public class Test {
        public static void main(String[] args) {
            File dir = new File("./");
            try(Waler waler = WalerFactory.open(dir)) {
                Wal wal = waler.append("begin;");
                waler.append("update `order` set amount = 1000 where id = 1;");
                waler.append("commit;");
                waler.sync();
            }
        }
    }
```
- Fetch a wal
```java
    import org.walog.Wal;
    import org.walog.Waler;
    import org.walog.WalerFactory;
    
    public class Test {
        public static void main(String[] args) {
            File dir = new File("./");
            try(Waler waler = WalerFactory.open(dir)) {
                Wal wal = waler.first();
                System.out.println(wal);
                
                wal = waler.get(wal.getLsn());
                System.out.println(wal);
    
                wal = waler.next(wal);
                System.out.println(wal);
            }
        }
    }
```
- Iterate wal from begin of wal files
```java
    import org.walog.Wal;
    import org.walog.WalIterator;
    import org.walog.Waler;
    import org.walog.WalerFactory;
    
    public class Test {
        public static void main(String[] args) {
            File dir = new File("./");
            try(Waler waler = WalerFactory.open(dir)) {
                try (WalIterator it = waler.iterate()) {                    
                    while (it.hasNext()) {
                        Wal wal = it.next();
                        System.out.println(wal);
                    }
                }
            }
        }
    }
```
- Iterate wal from the specified lsn
```java
    import org.walog.Wal;
    import org.walog.WalIterator;
    import org.walog.Waler;
    import org.walog.WalerFactory;
    
    public class Test {
        public static void main(String[] args) {
            File dir = new File("./");
            long lsn; // init lsn from last storing
            try(Waler waler = WalerFactory.open(dir)) {
                try (WalIterator it = waler.iterate(lsn)) {                    
                    while (it.hasNext()) {
                        Wal wal = it.next();
                        System.out.println(wal);
                    }
                }
            }
        }
    }
```
