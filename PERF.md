# Performance Testing

## Mobibench

1. Clone Mobibench source repository:

```console
git clone git@github.com:ESOS-Lab/Mobibench.git
```

2. Patch Mobibench:

```patch
diff --git a/shell/Makefile b/shell/Makefile
index 6b65351..262ab5f 100644
--- a/shell/Makefile
+++ b/shell/Makefile
@@ -4,8 +4,7 @@
 
 EXENAME = mobibench
 
-SRCS = mobibench.c \
-         sqlite3.c
+SRCS = mobibench.c
 
 INSTALL = install
 
@@ -14,6 +13,8 @@ bindir = $(prefix)/bin
 
 CFLAGS = -lpthread -ldl
 
+LIBS = <limbo-git-tree>/target/release/liblimbo_sqlite3.a -lm
+
 #CFLAGS += -DDEBUG_SCRIPT
 
 #for sqltie3
@@ -37,7 +38,7 @@ CFLAGS += -DNDEBUG=1 \
      --static
 
 all :
-       $(CROSS)gcc -o $(EXENAME) $(SRCS) $(CFLAGS)
+       $(CROSS)gcc -o $(EXENAME) $(SRCS) $(CFLAGS) $(LIBS)
 
 clean :
        @rm -rf mobibench
```

3. Build Mobibench:

```console
cd shell && make
```

4. Run Mobibench:

```console
./mobibench -p <benchmark-directory> -n 1000 -d 0
```
