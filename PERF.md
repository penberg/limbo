# Performance Testing

## Mobibench

1. Clone the source repository of Mobibench fork for Limbo:

```console
git clone git@github.com:penberg/Mobibench.git
```

2. Change `LIBS` in `shell/Makefile` to point to your Limbo source repository.

3. Build Mobibench:

```console
cd shell && make
```

4. Run Mobibench:

```console
./mobibench -p <benchmark-directory> -n 1000 -d 0
```
