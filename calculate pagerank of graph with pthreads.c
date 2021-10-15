#include <pthread.h>
#include <stdio.h>
#include<stdlib.h>
#include <semaphore.h>
#include<time.h>
#include <unistd.h> 
pthread_barrier_t   barrier;
struct dest{
    int dest;
    struct dest* next;
};

struct graphList
{
    int src;
    struct graphList *next;
    struct dest *destHead;
    
};

struct pagerankList{
    int id,edges;
    float pagerank,prevPagerank;
    struct pagerankList* next;
};

struct graphList *head = NULL;

struct pagerankList *pagerankHead=NULL;



void increaseEdges(int id){
    struct pagerankList* temp=pagerankHead;
    while(temp){
        if(temp->id==id)
            temp->edges++;
        temp=temp->next;
    }
}

struct pagerankList* createPagerank(int id){
    struct pagerankList *newNode = malloc(sizeof(struct pagerankList));
    newNode->next = NULL;
    newNode->id = id;
    newNode->edges=0;
    newNode->pagerank=1.000;
    newNode->prevPagerank=1.000;
    return newNode;
}

int searchPagerank(int id){
    struct pagerankList* temp=pagerankHead;
    while(temp&&temp->id!=id)
        temp=temp->next;

    if(temp) return 0;

    return 1;
}

void pagerankInsert(int id){
    
    if(!pagerankHead)
        pagerankHead=createPagerank(id);
    else if(searchPagerank(id)){
            struct pagerankList* newNode=createPagerank(id);
            newNode->next=pagerankHead;
            pagerankHead=newNode;
    }
}
struct graphList *createNode(int src)
{
    struct graphList *newNode = malloc(sizeof(struct graphList));
    newNode->next = NULL;
    newNode->src = src;
    newNode->destHead =NULL;
    return newNode;
}
struct dest* insertDestNode(int dest,struct dest* head){
    if(!head){
        head=malloc(sizeof(struct dest));
        head->next=NULL;
        head->dest=dest;
    }else{
        struct dest* newNode=malloc(sizeof(struct dest));
        newNode->dest=dest;
        newNode->next=head;
        head=newNode;
    }
    return head;
}

struct graphList* searchInGraph(int src){
    struct graphList* temp=head;
   
    while(temp&&temp->src!=src)
        temp=temp->next;
    
    if(temp) return temp;

    return NULL;
}
void insert(int src, int dest)
{
    if (!head){
        head = createNode(src);
        head->destHead=insertDestNode(dest,head->destHead);
        increaseEdges(src);
    }
    else{
        struct graphList* newNode=searchInGraph(src);
        if(!newNode){
            newNode=createNode(src);
            newNode->destHead=insertDestNode(dest,newNode->destHead);
            increaseEdges(src);
            newNode->next=head;
            head=newNode;
        }
        else{
            newNode->destHead=insertDestNode(dest,newNode->destHead);
            increaseEdges(src);
         
        }
        
    }
}

void print()
{
    FILE * fp;
    fp=fopen("pagerank1.csv","w+");
    fprintf(fp,"node,pagerank\n");
   /* struct graphList *temp = head;
    while (temp)
    {
        printf("node with id: %d has neighbours:\n", temp->src);
        
        struct dest *Tmp = temp->destHead;
        while (Tmp)
        {
            printf(" %d ", Tmp->dest);
            Tmp = Tmp->next;
        }
        printf("\n");
        temp = temp->next;
    }
    printf("//////////////////////////////////////////////////\n"); */
    struct pagerankList *tmp = pagerankHead;
    while (tmp)
    {
       // printf("node with id: %d has pagerank: %.3f and number of edges: %d\n", tmp->id, tmp->pagerank, tmp->edges);
        fprintf(fp,"%d , %.3f\n",tmp->id,tmp->pagerank);
        tmp = tmp->next;
    }
}



int checkIfNeighbour(int id,struct dest* neighbours){
    struct dest* tmp=neighbours;
    while(tmp&&tmp->dest!=id)
        tmp=tmp->next;
   
    if(tmp) return 1;

    return 0;
}

void decreasePagerank(int src)
{
    struct pagerankList *tmp = pagerankHead;
    while (tmp && tmp->id != src)
        tmp = tmp->next;
    if (tmp)
    {
        float pagerank = 0.85 * tmp->pagerank;
        tmp->prevPagerank=tmp->pagerank;
        tmp->pagerank = tmp->pagerank - pagerank;
      
    }
}

void increasePagerank(int src, struct dest *neighbours)
{
    struct pagerankList *tmp = pagerankHead;
    while (tmp && tmp->id != src)
        tmp = tmp->next;
    if (tmp)
    {
     
        float pagerank=tmp->prevPagerank;
        pagerank=pagerank*0.85;
      
        struct pagerankList *temp = pagerankHead;
        while (temp)
        {   
            
            if (checkIfNeighbour(temp->id, neighbours))
                temp->pagerank = temp->pagerank+pagerank/(float)tmp->edges;

            temp = temp->next;
        }
    }
}

void calculatePagerank(struct graphList* from,struct graphList *to)
{
	pthread_barrier_wait (&barrier);
    struct graphList *temp = from;
    while (temp!=to)
    {
        decreasePagerank(temp->src);
        temp = temp->next;
    }
    
    pthread_barrier_wait (&barrier);
    temp = from;
    while (temp!=to)
    {
        increasePagerank(temp->src, temp->destHead);
        temp = temp->next;
    }
}
void readfile()
{

    char const *const fileName = "Email-Enron.txt";
    FILE *file = fopen(fileName, "r");
    FILE *file1 = fopen(fileName, "r");

    if (!file)
    {
        printf("\n Unable to open : %s ", fileName);
        exit(-1);
    }

    char line[500];
    char y;
    int x, first;
    int count;
    int flag = 0;
    while (((count = fscanf(file, "%ld", &x)) != EOF) && (fscanf(file1, "%c", &y)) != EOF)
    {


        if (count == 1) //an einai int tha gurisei 1
        {
            pagerankInsert(x);
            flag++;
            if (flag == 2)
            {
                insert(first, x);
                flag = 0;
            }
            else
                first = x;
        }
        else           // an de gurisei 1 einai char
        {
            if (y == '#') // an o char einai o # kane consume mexri na vreis new line
            {
                do
                {
                    fgetc(file);
                    y = fgetc(file1);
                } while (y != '\n');
            }
            else  // alliws kane consume ena aplo char
                fgetc(file);
        }
    }

    fclose(file);
}
struct graphList* getMiddle(struct graphList *begin,struct graphList *end)
{
    struct graphList *slow_ptr = begin;
    struct graphList *fast_ptr = begin;
 
    if (begin!=NULL&&begin!=end)
    {
        while (fast_ptr != end && fast_ptr->next != end)
        {
            fast_ptr = fast_ptr->next->next;
            slow_ptr = slow_ptr->next;
        }
        
        
    }
    return slow_ptr;
}
struct graphList* get_tail(){
	struct graphList *temp = head;
    while (temp->next!=NULL)
        temp = temp->next;
	return temp;
}

void * first_thr(){
	struct graphList* middle=getMiddle(head,get_tail());
	struct graphList* middle1=getMiddle(head,middle);
	int i=0;

    for( i=0;i<50;i++)
    	calculatePagerank(head,middle1->next);
   
}
void * second_thr(){
	struct graphList* middle=getMiddle(head,get_tail());
	struct graphList* middle1=getMiddle(head,middle);
	int i=0;

    for( i=0;i<50;i++)
    	calculatePagerank(middle1->next,middle->next);
}

void* third_thr(){
	struct graphList* middle=getMiddle(head,get_tail());
	struct graphList* middle1=getMiddle(middle,get_tail());

	int i=0;
    for( i=0;i<50;i++)
    	calculatePagerank(middle->next,middle1->next);
}
void* fourth_thr(){
	struct graphList* middle=getMiddle(head,get_tail());
	struct graphList* middle1=getMiddle(middle,get_tail());
	int i=0;
    for( i=0;i<50;i++)
    	calculatePagerank(middle1->next,NULL);
}

int main(int argc, char *argv[])
{
	readfile();
	pthread_t first,second,third,fourth;
	pthread_barrier_init (&barrier, NULL, 4);
	struct timeval start, end;
    gettimeofday(&start,NULL);
	printf("method with 4 threads\n");
   	
    pthread_create(&first, NULL, first_thr, NULL);
    pthread_create(&second, NULL, second_thr, NULL);
    pthread_create(&third, NULL, third_thr, NULL);
    pthread_create(&fourth, NULL, fourth_thr, NULL);
    pthread_join(first, NULL);
    pthread_join(second, NULL);
    pthread_join(third, NULL);
    gettimeofday(&end,NULL);
    long seconds=(end.tv_sec-start.tv_sec);
    long micros = ((seconds*1000000)+end.tv_usec)-start.tv_usec;
    printf("Time used in microseconds : %ld\n",micros); 
    print();
    
   
    return 0;
}
