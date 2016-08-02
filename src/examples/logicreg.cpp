#include <iostream>
#include "libfaster.h"

using namespace std;
using namespace faster;

using std::chrono::system_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

int numDimensions = 10;
double * globalWeights;

pair<int,vector<float>> toVector(string & input){

	stringstream ss(input);
	vector<float> position;
	position.reserve(10);
	int key;
	float val;

	ss >> key;


	while(ss >> val){
		position.insert(position.end(), val);
	}

	return make_pair(key, move(position));
}

double dotp(double * w, vector<float> & v){
	double r = 0;

	for ( size_t i = 0; i < v.size(); i++)
		r += w[i]*v[i];
	return r;
}

vector<double> getGradient(const int & y, vector<float> & x){
	vector<double> ret(x.size());

	double e = y * (1 / (1 + exp( -y * dotp(globalWeights, x))) - 1);


	for ( size_t i = 0; i < x.size(); i++){
		ret[i] = e * x[i];
		printf("%lf ", ret[i]);
	}

	return ret;
}

vector<double> sumGradient(vector<double> & a, vector<double> & b){
	vector<double> ret(a.size());

	for ( size_t i = 0; i < a.size(); i++ ){
		ret[i] = a[i] + b[i];
	}

	return ret;
}

int main(int argc, char ** argv){
	int numItems;
	double error = 0;
	// Init Faster Framework
	auto start = system_clock::now();

	if (argc >= 3) numDimensions = atoi(argv[2]);

	fastContext fc(argc, argv);
	fc.registerFunction((void*) &toVector, "toVector");
	fc.registerFunction((void*) &getGradient, "getGradient");
	fc.registerFunction((void*) &sumGradient, "sumGradient");

	fc.registerGlobal(&globalWeights, numDimensions*sizeof(double));
	fc.startWorkers();
	if (!fc.isDriver())
		return 0;

	cerr << "------------ Logic Reg -------------\n";

	fc.printHeader();
	cerr << "Init Time: " << duration_cast<milliseconds>(system_clock::now() - start).count() << "ms\n";

	globalWeights = new double[numDimensions];


	cerr << "Import Data\n";
	auto start2 = system_clock::now();
	auto strdata = new fdd<string>(fc, argv[1]);
	cerr << "  Read Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";

	cerr << "Convert to Vector\n";
	auto data = strdata->map(&toVector)->cache();
	numItems = data->getSize();
	fc.updateInfo();
	cerr << numItems << " items" << '\n';

	vector<double> gradient(numDimensions);
	vector<double> oldGradient(numDimensions);
	for ( int i = 0; i < numDimensions; i++ ){
		globalWeights[i] = 10;
	}

	cerr << "Process Data\n";
	int i = 0;
	while( i < 10 ){
		cerr << "\033[1;32mIteration " << i++ << "\033[0m\n" ;
		start2 = system_clock::now();

		gradient = data->map(&getGradient)->reduce(&sumGradient);
		fc.updateInfo();
	cout << "Gradient: ";
	for ( double x : gradient )
		cout << x << " ";
	cout << "\n";

		for ( int i = 0; i < numDimensions; i++ ){
			globalWeights[i] -= gradient[i];
		}
		//error = subtract(gradient, oldGradient);
		//oldGradient = gradient;
		cerr << "  Error " << error << " time:" << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	}

	start2 = system_clock::now();

	cout << "Gradient: ";
	for ( double x : gradient )
		cout << x << " ";
	cout << "\n";

	auto duration = duration_cast<milliseconds>(system_clock::now() - start).count();
	cerr << "  Write Time: " << duration_cast<milliseconds>(system_clock::now() - start2).count() << "ms\n";
	cerr << "LogicReg in " << data->getSize() << " points in "<< i << " iterations! In " << duration << "ms (error: " << error <<  ") \n";
}
