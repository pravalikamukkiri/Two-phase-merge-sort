#include<bits/stdc++.h>
#include <sys/stat.h> 
#include <sys/types.h> 
#include<fstream>
#include<ctime>
#include<ratio>
#include<chrono>
using namespace std;
#define intl long long

void error(string msg="Error"){
	cout<<msg<<endl;
	exit(-1);
}

vector<intl> order_of_indices;
intl asc;
intl files_count=0;

map<string,intl> map_of_metadata(){
	map<string,intl> mp;
	ifstream file;
	file.open("metadata.txt");
	string line,token,key,val;

	while(file >> line){
		stringstream ss(line);
		getline(ss,key,',');
		getline(ss,val,',');
		mp[key]=atoi(val.c_str());
		cout<<key<<" "<<mp[key]<<endl;
	}
	file.close();
	return mp;
}

map<string,intl> get_order_of_columns(){
	map<string,intl> mp;
	ifstream file;
	file.open("metadata.txt");
	string line,token,key,val;
	intl i=0;
	while(file >> line){
		stringstream ss(line);
		getline(ss,key,',');
		getline(ss,val,',');
		mp[key]=i++;
	}
	file.close();
	return mp;

}

vector<intl> get_order_indices(intl argc,char * argv[],map<string,intl>mp){
	vector<intl> ord_ind;
	for(intl i=6;i<argc;i++)
		ord_ind.push_back(mp[argv[i]]);
	return ord_ind;
}

intl check_file(string file_name){
	std::ifstream infile(file_name);
    return infile.good();
}

vector<string> get_input_columns(intl argc,char *argv[], map<string,intl>&mp){
	vector<string> vect;
	for(intl i=6;i<argc;i++){
		string c=argv[i];
		if(mp.find(c)==mp.end())
			error("error in given columns");
		vect.push_back(c);
	}
	return vect;
}

intl get_record_size(map<string,intl> &mp){
	intl sum=0;
	for(auto it=mp.begin();it!=mp.end();it++)
		sum+=it->second;
	sum+=(mp.size())*2;
	return sum;

}



bool comp(vector<string> &a, vector<string> &b){
	for(intl i=0;i<order_of_indices.size();i++){
		intl x=order_of_indices[i];
		if(a[x]!=b[x])
			return asc==1 ? a[x]<b[x] : a[x]>b[x];
	}
	return 0;
}

intl get_tot_num_of_records(string inp_file){	
	string line;
	intl n = 0;
	ifstream file;
	file.open(inp_file);

	while(getline(file, line)){
		n++;
	}
	return n;
}

bool comp2(const pair<vector<string>,intl>&a, const pair<vector<string>,intl>&b){
	for(intl i=0;i<order_of_indices.size();i++){
		intl x=order_of_indices[i];
		if(a.first[x]!=b.first[x])
			return asc==1 ? a.first[x]>b.first[x] : a.first[x]<b.first[x];
	}
	return a.second > b.second;

}

pair<vector<intl>,vector<intl>> get_data_sizes(){
	vector<intl> sizes,pref_sizes;
	ifstream file;
	file.open("metadata.txt");
	string line,token,key,val;

	while(file >> line){
		stringstream ss(line);
		getline(ss,key,',');
		getline(ss,val,',');
		sizes.push_back(stoi(val));
		if(pref_sizes.size()==0)
			pref_sizes.push_back(stoi(val));
		else
			pref_sizes.push_back(pref_sizes[pref_sizes.size()-1]+stoi(val)+2);
	}
	return {sizes,pref_sizes};
}

void threading(intl start,intl end, intl filenum, vector<vector<string>> data_v){
	// if(files_count+filenum==1)
	// 	filenum=6;
	cout<<"t:  "<<files_count+filenum+1<<" "<<data_v.size()<<" "<<start<<" "<<end<<endl;
	ofstream blockfile("./temp/file"+to_string(files_count+filenum+1)+".txt");
	vector<vector<string>> temp;
	for(intl i=start;i<end;i++)
		temp.push_back(data_v[i]);
	sort(temp.begin(),temp.end(),comp);

	vector<string> sorted_datat;

	for(intl j=0;j<temp.size();j++){
		string temps;
		for(intl k=0;k<temp[j].size();k++){
			if(temps!="")
				temps+="  ";
			temps+=temp[j][k];
		}
		sorted_datat.push_back(temps);
	}
	cout<<files_count+filenum+1<<" "<<sorted_datat.size()<<endl;
	stringstream ssf;
	copy(sorted_datat.begin(),sorted_datat.end(),ostream_iterator<string>(ssf,"\r\n"));

	blockfile<<ssf.rdbuf();
	blockfile.flush();
	blockfile.close();
}



int main(int argc, char* argv[]){

	time_t start_time=time(NULL);

	if(argc <2)
		error("Input is not provided");
	if(argc<7)
		error("Not enough arguments.");

	string inp_file=argv[1];
	string out_file=argv[2];
	string order=argv[5];
	intl order_val=1;

	intl tot_num_of_threads=atoi(argv[4]);

	if(!check_file(inp_file))
		error("Input file does not exists :(");
	if(!check_file("metadata.txt"))
		error("Metadata file does not exists :(");
	if(order=="desc")
		asc=-1;
	else if(order=="asc")
		asc=1;
	else
		error("order is not specified");


	map<string,intl> meta_data = map_of_metadata();
	vector<string> input_columns = get_input_columns(argc,argv,meta_data);
	auto sizes_pair = get_data_sizes();
	vector<intl> sizes=sizes_pair.first;
	vector<intl> pref_sizes=sizes_pair.second;

	map<string,intl> order_cols = get_order_of_columns();
	order_of_indices.clear();
	order_of_indices = get_order_indices(argc,argv,order_cols);

	intl rec_size=get_record_size(meta_data);

	ifstream inpf(inp_file);
	inpf.seekg(0,inpf.end);
	intl inpt_file_size=inpf.tellg();
	inpf.seekg(0,inpf.beg);

	if(inpt_file_size % rec_size)
		error("file is not a multiple of each record");

	intl line_count = inpt_file_size / rec_size ;

	intl mm=((intl)min(stoi(argv[3]),5))*(intl)pow(10,6);;
	cout<<mm<<endl;
	

	intl row_size=rec_size;
	intl num_of_rows = mm/row_size;
	intl file_size = num_of_rows*row_size;
	intl input_rows = line_count;

	intl num_of_files = input_rows / num_of_rows;

	if(input_rows%num_of_rows)
		num_of_files++;

	cout<<"fd"<<endl;
	cout<<inpt_file_size<<endl;
	cout<<row_size<<endl;
	cout<<num_of_rows<<endl;
	cout<<file_size<<endl;
	cout<<num_of_files<<endl;


	cout<<"Phase1 started"<<endl;

	if(system("rm -r temp"))
		cout<<"c"<<endl;

	mkdir("temp",0777);

	ifstream input(inp_file);

	files_count=0;

	for(intl i=0;i<num_of_files;i++){
		// dividing and writing the data intlo each file.
		intl rows_to_read=0;
		if(i==num_of_files-1)
			rows_to_read=input_rows - i*num_of_rows;
		else
			rows_to_read=num_of_rows;

		intl data_size=rows_to_read*row_size;
		char *data = new char[data_size+1];
		input.read(data,data_size);
		data[data_size]='\0';

		vector<vector<string>> data_vect;

		stringstream ss(data);
		// ofstream blockfile("./temp/file"+to_string(i+1)+".txt");

		// if(!blockfile.good())
		// 	error("Eroor in writing to files");

		string line;
		for(intl j=0;j<rows_to_read;j++){
			vector<string> temp_d;
			getline(ss,line);
			for(intl k=0;k<sizes.size();k++){
				string temp;
				if(k==0)
					temp_d.push_back(line.substr(0,sizes[k]));
				else
					temp_d.push_back(line.substr(pref_sizes[k-1]+2,sizes[k]));
			}
			data_vect.push_back(temp_d);
		}

		vector<string> sorted_data;
		int num_of_threads = min(tot_num_of_threads, rows_to_read);

		vector<std :: thread> thrds;

		for(intl j=0;j<num_of_threads;j++){
			intl start = (j*rows_to_read)/num_of_threads;
			intl end = ((j+1)*rows_to_read)/num_of_threads;
			thrds.push_back(std::thread(threading,start,end,j,data_vect));
		}

		for(auto &x:thrds)
			x.join();
		files_count+=num_of_threads;

		// sort(data_vect.begin(),data_vect.end(),comp);
		// for(intl j=0;j<data_vect.size();j++){
		// 	string temp;
		// 	for(intl k=0;k<data_vect[j].size();k++){
		// 		if(temp!="")
		// 			temp+="  ";
		// 		temp+=data_vect[j][k];
		// 	}
		// 	sorted_data.push_back(temp);
		// }
		// stringstream ssf;
		// copy(sorted_data.begin(),sorted_data.end(),ostream_iterator<string>(ssf,"\r\n"));

		// blockfile<<ssf.rdbuf();
		// blockfile.flush();
		// blockfile.close();
		// cout<<"Sorted and written to "<<i+1<<" file"<<endl;
		intl c=0;
		for(intl k=0;k<100000;k++)
			c++;
	}
	// return 0;



	cout<<"Phase 2 started:  "<<files_count<<endl;
	num_of_files=files_count;

	vector<vector<string>> vect[num_of_files];
	vector<pair<vector<string>,intl>> heap;
	vector<intl> tp_sizes(num_of_files);
	intl temp_rows=num_of_rows/(num_of_files+1);
	cout<<"temp_rows"<<temp_rows<<endl;
	ifstream block_files[num_of_files];


	for(intl i=0;i<num_of_files;i++){
		string file_p="./temp/file"+to_string(i+1)+".txt";
		// cout<<file_p<<endl;
		block_files[i].open(file_p);

		if(!block_files[i].good())
			error("error in "+file_p);
		intl rows_to_read;
		block_files[i].seekg(0,block_files[i].end);
		intl x=block_files[i].tellg()/row_size;
		tp_sizes[i]=x;
		block_files[i].close();

		
		rows_to_read=min(temp_rows,x);

		if(rows_to_read==0)
			continue;

		block_files[i].open(file_p);

		tp_sizes[i]-= rows_to_read;
		// cout<<i<<" r-"<<tp_sizes[i]<<endl;

		intl data_size=rows_to_read*row_size;
		char *data = new char[data_size+1];
		block_files[i].read(data,data_size);
		data[data_size]='\0';



		stringstream ss(data);
		string line;
		int j=0;
		while(getline(ss,line)){
			vector<string> temp_d;
			for(intl k=0;k<sizes.size();k++){
				string temp;
				if(k==0)
					temp_d.push_back(line.substr(0,sizes[k]));
				else
					temp_d.push_back(line.substr(pref_sizes[k-1]+2,sizes[k]));
			}
			vect[i].push_back(temp_d);
		}
		heap.push_back({vect[i][0],i});
		vect[i].erase(vect[i].begin());
		push_heap(heap.begin(),heap.end(),comp2);
	}

	vector<string> f_data;
	ofstream output(out_file);

	while(heap.size()){
		string top_ele;
		intl ind=heap[0].second;
		for(intl i=0;i<heap[0].first.size();i++)
			if(i==0)
				top_ele=heap[0].first[i];
			else
				top_ele+="  "+heap[0].first[i];
		f_data.push_back(top_ele);

		if(f_data.size()==temp_rows){
			stringstream output_f;
			copy(f_data.begin(),f_data.end(),ostream_iterator<string>(output_f,"\n"));
			output<<output_f.rdbuf();
			output_f.flush();
			f_data.clear();
		}
		pop_heap(heap.begin(),heap.end(),comp2);
		heap.pop_back();
		
		if(vect[ind].size()==0){
			intl rows_to_read=min(temp_rows,tp_sizes[ind]);
			if(rows_to_read==0)
				continue;
			tp_sizes[ind]-=rows_to_read;

			intl data_size=rows_to_read*row_size;
			char *data = new char[data_size+1];
			block_files[ind].read(data,data_size);
			data[data_size]='\0';

			stringstream ss(data);
			string line;

			for(intl j=0;j<rows_to_read;j++){
				vector<string> temp_d;
				getline(ss,line);
				for(intl k=0;k<sizes.size();k++){
					string temp;
					if(k==0){
						temp_d.push_back(line.substr(0,sizes[k]));
					}
					else
						temp_d.push_back(line.substr(pref_sizes[k-1]+2,sizes[k]));
				}
				vect[ind].push_back(temp_d);
			}
		}
		if(vect[ind].size()){
			heap.push_back({vect[ind][0],ind});
			push_heap(heap.begin(),heap.end(),comp2);
			vect[ind].erase(vect[ind].begin());
		}
	}
	stringstream output_f;
	copy(f_data.begin(),f_data.end(),ostream_iterator<string>(output_f,"\n"));
	output<<output_f.rdbuf();
	output_f.flush();
	f_data.clear();
	cout<<"Done :)"<<endl;

	time_t end_time =time(NULL);
	time_t time_taken = end_time - start_time;
	time_t time_mins = time_taken/60;
	time_t time_secs = time_taken%60;

	cout<<"Time taken: "<<endl;
	// cout<<start_time<<" "<<end_time<<" "<<time_taken<<endl;

	cout<<time_mins<<" mins  "<<time_secs<<" secs"<<endl;

}
