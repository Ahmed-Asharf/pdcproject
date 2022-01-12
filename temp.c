#include<stdio.h>
#include<stdlib.h>
#include<mpi.h>
#include<string.h>

int main(int argc, char *argv[]){
    char keys[argc][10];
    char values[argc][50];
    int world;
    int rank;
    int num = 10;
    char globalresults[100][1024];
    int threshold;
    for(int x = 1 ; x<=argc-1; x+=2)
    {
        strcpy(keys[x-1],argv[x]);
        strcpy(values[x-1], argv[x+1]);
        strcat(keys[x-1],"\0");
        strcat(values[x-1],"\0");
    }
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &world);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    char results[100][1024] = {"\0"};
    int index1=0;
    if(rank == 0)
    {
        threshold = (int)num/world;
        char line[2048];
        int pos = 0;
        for(int i = 1 ; i<=world-1;i++)
        {
            MPI_Send(&threshold,1,MPI_INT,i,1,MPI_COMM_WORLD);
        }
        FILE* stream = fopen("file1.txt","r");
        if(stream == NULL)
        {
            printf("Cannot open file");
            exit(0);
        }
        for(int i = 1 ; i<=world-1;i++)
        {
            char records[threshold][9][50];
            for(int j = 0 ; j < threshold; j++)
            {
                fgets(line, 1048, stream);
                char *token = strtok(line, ",");
                int x = 0;
                while (token != NULL)
                {
                    strcpy(records[j][x], token);
                    strcat(records[j][x], "\0");
                    token = strtok(NULL, ",");
                    x++;
                }
            }
            MPI_Send(records, threshold*9*50, MPI_CHAR, i, 1, MPI_COMM_WORLD);
        }
    }
    else
    {
        MPI_Recv(&threshold,1,MPI_INT,0,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        char local[threshold][9][50];
        MPI_Recv(local, threshold*9*50, MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        // printf("%d ",rank);
        // for(int i = 0 ; i<threshold ; i++)
        // {
        //     for(int j = 0 ; j < 9 ; j++)
        //     {
        //         printf("%s ",local[i][j]);
        //     }
        //     printf("\n");
        // }
        for(int i = 0  ; i<=argc-1 ; i+=2)
        {
         if(strcmp(keys[i],"name")==0){
            for(int x = 0; x < threshold; x++){
                    if(strcmp(local[x][0],values[i])==0)
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
        if(strcmp(keys[i],"cnic")==0){
            for(int x = 0; x < threshold; x++){
                if(strcmp(local[x][2],values[i])==0)
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
        if(strcmp(keys[i],"age")==0){
            for(int x = 0; x < threshold; x++){
                if(strcmp(local[x][3],values[i])==0)
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
        }
 
        printf("\n");
        for(int i = 0;i<index1; i++)
             printf("%s\n",results[i]);
    }
    MPI_Finalize();
}