# walog
A simple and high performance WAL implementation, supports multi-thread or multi-process iterating/appending log.
- Single thread appends 200,000+ items per second, iterates 1000,000+ items per second
- Support multi-processes iterate/append logs
- Simple API such as append(log)/first()/get(lsn)/next(log)/iterator()/iterator(lsn)/sync() etc
- Require JDK 7+

## examples
- Open wal logger
```java
    public class Test {
        public static void main(String[] args) {
            File dir = new File("./");
            try(Waler waler = WalerFactory.open(dir)){
                // iterate/append operations
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
