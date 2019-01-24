#include <iostream>  
#include "hdfs.h"  
#include <stdlib.h>  
#include <string.h>  
   
using namespace std;  
   
int main(int argc, char **argv)
{   
    //连接HDFS  
    hdfsFS fs = hdfsConnect(argv[0], 8020);
    if (!fs){  
        cout <<"cannot connect to HDFS"<<endl;  
        exit(1);  
    }  
    cout <<"connect success!" <<endl;  
    //检查写入的文件是否存在  
    int a = hdfsExists(fs,argv[1]);  
    cout <<a<<endl;  
    hdfsFile writeFile;  
    if (a == 0){  
        //如果文件存在，打开文件，并且设置项文件中追加数据  
        writeFile = hdfsOpenFile(fs, argv[1], O_WRONLY|O_APPEND, 0, 0, 0);  
    }else{  
        //如果文件不存在，则创建文件，并将数据写入  
        writeFile = hdfsOpenFile(fs, argv[1], O_WRONLY|O_CREAT, 0, 0, 0);  
    }  
    if (!writeFile){  
        cout <<"Faile to open the file"<<endl;  
        exit(1);  
    }  
    //将数据写入到HDFS中  
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)argv[2], strlen(argv[2])+1);  
    if (hdfsFlush(fs, writeFile)) {  
        cout << "Failed to flush" << endl;  
        exit(1);  
    }  
    //关闭连接  
    hdfsCloseFile(fs, writeFile);   
};
