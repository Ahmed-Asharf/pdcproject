#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>


int main()
{
    MPI_Init(NULL, NULL);
    int rank = 0;
    int world;
    int num = 500;
    char globalresults[100][1024];
    MPI_Comm_size(MPI_COMM_WORLD, &world);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // printf("%d\n",world);
    char results[100][1024];
    int index1=0;
    int threshold = (int)num/world;
    if(rank == 0){ //master
        //divide the input file
        FILE* stream = fopen("file1.txt", "r");
        char line[2048];
        int count = 0;
        int y;
        int pos = 0;
        int z;
        for(y = 0; y < world-1; y++){
            char records[threshold][9][50];
            for(z = 0; z < threshold; z++){
                fgets(line, 2048, stream);
                char* tmp = strdup(line);
                char * token = strtok(line, ",");
                int x = 0;
                while( token != NULL ) {
                    strcpy(records[z][x], token);
                    strcat(records[z][x], "\0");
                    token = strtok(NULL, ",");
                    x++;
                }
                free(tmp);
            }
            MPI_Send(records, threshold*9*50, MPI_CHAR, y+1, 0, MPI_COMM_WORLD);
        }
    }
    else{
        char local[threshold][9][50];
        MPI_Recv(&local, threshold*9*50, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        int x, y;
        for(x = 0; x < threshold; x++){
            if(strcmp(local[x][0],"Sir Mir Khan")==0)
            {
                char *str1 = local[x][0];
                for(int i = 1 ; i < 9 ; i++)
                {
                    strcat(str1," ");
                    strcat(str1, local[x][i]);
                   
                }
                strcat(str1,"\0");
                // printf("%s\n", str1);
                strcpy(results[index1],str1); 
                index1++; 
            }
            else if(strcmp(local[x][1],"Muhammad Mureed")==0)
            {
                               char *str1 = local[x][0];
                for(int i = 1 ; i < 9 ; i++)
                {
                    strcat(str1," ");
                    strcat(str1, local[x][i]);
                   
                }
                strcat(str1,"\0");
                // printf("%s\n", str1);
                strcpy(results[index1],str1);  
                index1++; 
            }
            else if(strcmp(local[x][2],"68632-26254834-4")==0)
            {
                char *str1 = local[x][0];
                for(int i = 1 ; i < 9 ; i++)
                {
                    strcat(str1," ");
                    strcat(str1, local[x][i]);
                   
                }
                strcat(str1,"\0");
                strcpy(results[index1],str1);  
                index1++; 
            }
            else if(strcmp(local[x][3],"48")==0)
            {
                char *str1 = local[x][0];
                for(int i = 1 ; i < 9 ; i++)
                {
                    strcat(str1," ");
                    strcat(str1, local[x][i]);
                   
                }
                strcat(str1,"\0");
              strcpy(results[index1],str1);  
              index1++; 
            }

        }


    }
    MPI_Gather(&results[0], index1, MPI_CHAR, &globalresults,index1,MPI_CHAR,0,MPI_COMM_WORLD);
    if(rank == 0)
    {
        for(int i = 0;i<index1; i++)
            printf("%s\n",globalresults[i]);
        printf("%c\n",globalresults[0][5]);
    }
    MPI_Finalize();
}
