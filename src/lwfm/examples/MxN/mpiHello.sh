cd ~/mxn
mpicxx mpiHello.cxx -o mpiHello
mpirun -np 2 ./mpiHello > mpiHello.log

# Sleep to make sure this lives long enough for a trigger
sleep 60