//  main.cpp
//  DataMiningProject(7970 Big Data Mining)
//
//  Group 08
//  Created by Yunfan Rao on 11/26/17.
//  Copyright © 2017 Yunfan Rao. All rights reserved.

/********************************************************************\
 *                    Global system headers                           *
 \********************************************************************/
#include <iostream>
#include <fstream>
#include <set>
#include <map>
#include <math.h>
#include <queue>
#include <sys/time.h>

/******************************************************************\
 *                  Global data types                               *
 \******************************************************************/
typedef double          Timestamp;
typedef double          Period;

/**********************************************************************\
 *                      Global definitions                              *
 \**********************************************************************/
#define SIZE_e 0.70
#define SIZE_u 2

/**********************************************************************\
 *                      Global data                              *
 \**********************************************************************/
Timestamp StartTime;
std::set<int> v;
std::multimap<int, int> e;
std::set<int> cores;
std::multimap<int, int> classified;
std::set<int> hubs;
std::set<int> outliers;

/**********************************************************************\
 *                        Function prototypes                           *
 \**********************************************************************/
std::set<int> neighboursOfAVertex(int a_vertex);
std::set<int> e_neighboursOfAVertex(int a_vertex);
bool directReach(int coreV, int destV);
bool isCore(int coreV);

/*core methods*/
void readDataSetAndInitializeVandE(std::string fileName);
void creatCores();
void clustering();

/*count time methods*/
Timestamp Now();

/*print methods*/
void print_ASet(std::set<int> a_set);
void print_V(std::set<int> v);
void print_AMMap(std::multimap<int, int> e);

/*write output to files*/
void write_ASet(std::string fileName, std::set<int> a_set);
void write_AMMap(std::string fileName, std::multimap<int, int> e);

int main(int argc, const char * argv[]) {
    
    Timestamp StartTime;
    Period    runTime;
    //set the dataset file
    //std::string fileName = "xsmall_dataset";
    //std::string fileName = "CG1";
    std::string fileName = "CG2";
    //std::string fileName = "facebook_combined";
    //std::string fileName = "CA-GrQc";  // # of nodes: 5242, # of edges: 28980
    //std::string fileName = "com-dblp.ungraph"; //# of nodes: 317080, # of edges: 1049866

    std::string path = "/Users/yunfanrao/Desktop/datasets/";
    std::string inputfile = path + fileName  + ".txt";
    //std::string inputfile = path + argv[1];
    //read the dataset and initialize vertexes and edges
    readDataSetAndInitializeVandE(inputfile);
    
    StartTime = Now();
    
    //create cores
    creatCores();
    
    //do clustering
    clustering();
    
    runTime = Now() - StartTime;
    
    //print out all vertexes, edges, cores, hubs and outliers
//    std::cout << "*******************************" << std::endl;
//    std::cout << "Print out all vertexes:" << std::endl;
//    print_ASet(v);
//    std::cout << "Print out all edges:" << std::endl;
//    print_AMMap(e);
//    std::cout << "Print out all cores:" << std::endl;
//    print_ASet(cores);
    std::cout << "Print out all clusters:" << std::endl;
    std::cout << "vertexID - clusterID:" << std::endl;
    print_AMMap(classified);
    std::cout << "Print out all hubs:" << std::endl;
    print_ASet(hubs);
    std::cout << "Print out all outliers:" << std::endl;
    print_ASet(outliers);

    //print out the time of running
    std::cout << "The running time is: " << runTime << "s" << std::endl << std::endl;
    
    /*write the result to the output files*/
//    std::string outputFileForClassified = path + fileName + "_classified" + "_output.txt";
//    std::string outputFileForHubs = path + fileName + "_hubs" + "_output.txt";
//    std::string outputFileForOutliers = path + fileName + "_outliers" + "_output.txt";
    std::string outputFileForClassified = "_classified_output.txt";
    std::string outputFileForHubs = "_hubs_output.txt";
    std::string outputFileForOutliers = "_outliers_output.txt";
    write_AMMap(outputFileForClassified, classified);
    write_ASet(outputFileForHubs, hubs);
    write_ASet(outputFileForOutliers, outliers);
    
    return 0;
}

//*************************************************//
//Clustering the network(graph) based on the algorithm SCAN
//*************************************************//
void clustering() {
    std::set<int> unclassified;
    std::set<int> nonMember;
    std::set<int>::iterator it, it1,it2;
    std::queue<int> queueQ;
    
    int clusterID = -1;
    
    //make all vertexes unclassified
    unclassified = v;
    
    for(int each_v: v){
        // check if vertex is classified
        if(unclassified.find(each_v) == unclassified.end())
            continue;
        it = cores.find(each_v);
        if(it != cores.end()){  //each_v is a core vertex
            clusterID++;
            std::set<int> e_neighbors = e_neighboursOfAVertex(each_v);
            for(int nei_v: e_neighbors) // insert all e_neighbours into the queue
                queueQ.push(nei_v);
            while(!queueQ.empty()){ //iterate the queue
                int y = queueQ.front();
                queueQ.pop();
                if(isCore(y)){ //Direct reach should start at a core vertex
                    std::set<int> y_e_neighbors = e_neighboursOfAVertex(y);
                    for(int yNei_v:y_e_neighbors) {
                        it1 = unclassified.find(yNei_v);
                        it2 = nonMember.find(yNei_v);
                        if(it1 != unclassified.end() || it2 != nonMember.end()) { //yNei_v is unclassified or nonMember
                            classified.insert(std::make_pair(yNei_v, clusterID));
                            //unclassified.erase(yNei_v);
                            nonMember.erase(yNei_v);
                        }
                        it1 = unclassified.find(yNei_v);
                        if(it1 != unclassified.end()) { //yNei_v is unclassified
                            queueQ.push(yNei_v);
                            unclassified.erase(yNei_v);
                        }
                    }
                    
                }
                
            }
        }
        else
            nonMember.insert(each_v);
    }
    
    int clusterID1, clusterID2;
    bool tag;
    
    /*identify the hubs and the outliers*/
    for(int temp_v: nonMember) {
        tag = false;
        clusterID1 = -1;
        clusterID2 = -1;
        std::set<int> temp_neighbours = neighboursOfAVertex(temp_v);
        std::multimap<int,int>::iterator temp_it;
        for(int temp_x: temp_neighbours){
            temp_it = classified.find(temp_x);
            if(temp_it != classified.end()){ //if the temp_it is classified
                clusterID2 = temp_it->second;
            }
            if(clusterID1 < 0) { //clusterID haven't been assign
                clusterID1 = clusterID2;
            }
            //there is a pair of neighbours of temp_v which have different clusterID
            if(clusterID1 != clusterID2) {
                hubs.insert(temp_v);
                tag = true;
                break;
            }
        }
        //No a pair of neighbours of temp_v which have different clusterID
        if(tag == false)
            outliers.insert(temp_v);
    }
}

//*************************************************//
//Read a file and initialize vertexes set and edges set
//*************************************************//
void readDataSetAndInitializeVandE(std::string fileName) {
    std::set<int>::iterator it1, it2;
    std::fstream inputFile;
    inputFile.open(fileName, std::ios::in);
    
    /* initialize V and E in G<V,E> */
    if(inputFile) {
        int value1, value2;
        while(inputFile >> value1 >> value2){
            
            // if value1 doesn't exist in v, insert value1 into V
            it1 = v.find(value1);
            if (it1 == v.end()) {
                v.insert(value1);
            }
            
            // if value2 doesn't exist in v, insert value2 into V
            it2 = v.find(value2);
            if (it2 == v.end()) {
                v.insert(value2);
            }
            
            // insert value1 -> value2 and value2 -> value1 into E
            e.insert(std::pair<int, int>(value1, value2));
            e.insert(std::pair<int, int>(value2, value1));
        }
    }
    
}

//*************************************************//
//Return a set that contains the neighours of this vertex.
//*************************************************//
std::set<int> neighboursOfAVertex(int a_vertex) {
    std::set<int> neighbs;
    std::multimap<int, int>::iterator it;
    std::pair<std::multimap<int, int>::iterator, std::multimap<int, int>::iterator> range;
    
    neighbs.insert(a_vertex);
    
    range = e.equal_range(a_vertex);
    for(it = range.first; it != range.second; ++it)
        neighbs.insert(it -> second);
    return neighbs;
}

//*************************************************//
//Return a set that contains the e_neighours of this vertex.
//*************************************************//
std::set<int> e_neighboursOfAVertex(int this_v) {
    std::set<int> e_neighbs;
    std::set<int> this_v_neighbs;
    std::set<int> neighb_v_neighbs;
    std::set<int>::iterator it1,it2;
    int common_count;
    int sizeOfThisVNeighbs;
    int sizeOfNeighbVNeighbs;
    double similarity;
    
    this_v_neighbs = neighboursOfAVertex(this_v);
    sizeOfThisVNeighbs = this_v_neighbs.size();
    
    for(int neighb_v: this_v_neighbs){
        common_count = 0;
        neighb_v_neighbs = neighboursOfAVertex(neighb_v);
        sizeOfNeighbVNeighbs = neighb_v_neighbs.size();
        for(it1 = this_v_neighbs.begin(); it1 != this_v_neighbs.end(); it1++) {
            it2 = neighb_v_neighbs.find(*it1);
            if(it2 != neighb_v_neighbs.end())
                common_count++;
        }
        similarity = common_count / sqrt(sizeOfNeighbVNeighbs * sizeOfThisVNeighbs);
        if(similarity >= SIZE_e) {
            e_neighbs.insert(neighb_v);
        }
    }
    
    return e_neighbs;
}

//*************************************************//
//Creat core vertexes based on e_neighbours, u and e.
//*************************************************//
void creatCores() {
    
    for(int each_v: v) {
        //std::set<int> neighers = neighboursOfAVertex(each_v);
        std::set<int> e_neighers = e_neighboursOfAVertex(each_v);
        
        if(e_neighers.size() >= SIZE_u)
            cores.insert(each_v);
    }
}

//*************************************************//
//Return true if the first vertex(parameter)
//direct reach to the second, otherwise return false.
//*************************************************//
bool directReach(int coreV, int destV) {
    std::set<int>::iterator it;
    it = cores.find(coreV);
    if(it == cores.end())
        return false;
    std::set<int> e_neighers = e_neighboursOfAVertex(coreV);
    it = e_neighers.find(destV);
    if(it == e_neighers.end())
        return false;
    return true;
}

//*************************************************//
//Return true if the vertex is a core vertex,
//otherwise return false.
//*************************************************//
bool isCore(int coreV) {
    std::set<int>::iterator it;
    it = cores.find(coreV);
    if(it == cores.end())
        return false;
    return true;
}

/*********************************************************************\
 * Input    : None                                                    *
 * Output   : Returns the current system time                         *
 \*********************************************************************/
Timestamp Now(){
    struct timeval tv_CurrentTime;
    gettimeofday(&tv_CurrentTime,NULL);
    return( (Timestamp) tv_CurrentTime.tv_sec + (Timestamp) tv_CurrentTime.tv_usec / 1000000.0-StartTime);
}

//*************************************************//
//Print out all vertexes in the set
//*************************************************//
void print_ASet(std::set<int> a_set) {
    // print out all the vertexes from this set
    //std::cout << "*******************************" << std::endl;
    std::cout << "All vertexes in this set:" << std::endl << std::endl;
    for(int each: a_set) {
        std::cout << each <<std::endl;
    }
    std::cout << "\n#of nodes: " << a_set.size() << std::endl << std::endl;
    std::cout << "*******************************" << std::endl<< std::endl;
}

//*************************************************//
//Print out all edges or clustering relation in the multimap
//*************************************************//
void print_AMMap(std::multimap<int, int> e) {
    // print out all the edges from E
    //std::cout << "*******************************" << std::endl;
    //std::cout << "All edges in E:" << std::endl << std::endl;
    for(auto each_e: e) {
        std::cout << each_e.first << "\t-\t" <<each_e.second <<std::endl;
    }
    std::cout << "\n#of pairs: " << e.size() << std::endl << std::endl;
    std::cout << "*******************************" << std::endl<< std::endl;
}

//*************************************************//
//Write the result set to the file with fileName
//*************************************************//
void write_ASet(std::string fileName, std::set<int> a_set) {
    std::ofstream output(fileName);
    output << "All vertexes in this set:" << std::endl << std::endl;
    for (int each: a_set) {
        output << each << std::endl;
    }
    output << "\n#of nodes: " << a_set.size() <<std::endl;
    output << "*******************************" << std::endl<< std::endl;
    std:: cout << "Write set done!" << std::endl;
}

//*************************************************//
//Write the result multimap to the file with fileName
//*************************************************//
void write_AMMap(std::string fileName, std::multimap<int, int> e) {
    std::ofstream output(fileName);
    for(auto each_e: e) {
        output << each_e.first << "\t-\t" <<each_e.second <<std::endl;
    }
    output << "\n#of pairs: " << e.size() << std::endl;
    output << "*******************************" << std::endl<< std::endl;
    std:: cout << "Write multimap done!" << std::endl;
}
