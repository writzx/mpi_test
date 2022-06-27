## RUNNING THE PROGRAM
```
mpiexec -n <total ranks> ./mpi_test <total rows> <total cols> [<input file>]
```

The `<input file>` is an optional argument. If it's omitted, the input is taken from stdin.
#### Example
```
mpiexec -n 11 ./mpi_test 300 200 input.txt
```
#### or
```
mpiexec -n 11 ./mpi_test 300 200
```
### Example commands

```
GET ROW 23
get aggr 95
exit
```
