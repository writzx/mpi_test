#include <mpi.h>
#include <iostream>

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    // Get the number of processes
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Get the rank of the process
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    std::cout << "World Size: " << world_size << "   Rank: " << world_rank << std::endl;

    // Finalize the MPI environment.
    MPI_Finalize();

    return 0;
}
