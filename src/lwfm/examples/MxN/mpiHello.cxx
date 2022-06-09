#include <mpi.h>
#include <iostream>

int main() {
   MPI_Init(NULL, NULL);

   int proc;
   MPI_Comm_rank(MPI_COMM_WORLD, &proc);

   std::cout << proc << std::endl;

   MPI_Finalize();
}
