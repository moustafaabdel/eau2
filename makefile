make:
	- g++ -std=c++11 ./test/regression_personal_test_suite.cpp -o old_tests
	- g++ -std=c++11 ./test/QuickAcceptance/row_test.cpp -o row_test
	- g++ -std=c++11 ./test/QuickAcceptance/column_test.cpp -o column_test
	- g++ -std=c++11 ./test/QuickAcceptance/schema_test.cpp -o schema_test
	- g++ -std=c++11 ./test/QuickAcceptance/dataframe_test.cpp -o dataframe_test
	- g++ -std=c++11 ./test/QuickAcceptance/serial_test.cpp -o serial_test
	- g++ -std=c++11 ./test/QuickAcceptance/kvs_test.cpp -o kvs_test
	- g++ -std=c++11 ./application/demo.cpp -o demo_app
	- g++ -std=c++11 ./application/trivial.cpp -o trivial_app


clean:
	- rm old_tests
	- rm row_test
	- rm column_test
	- rm schema_test
	- rm dataframe_test
	- rm serial_test
	- rm kvs_test
	- rm demo_app
	- rm trivial_app


run:
	- ./old_tests
	- ./row_test
	- ./column_test
	- ./schema_test
	- ./dataframe_test
	- ./serial_test
	- ./kvs_test
	- ./trivial_app
	- ./demo_app

mem:

	# - valgrind --leak-check=<summary>  ./old_tests
	# - valgrind --leak-check=<summary>  ./row_test
	- valgrind  ./column_test
	# - valgrind --leak-check=<summary>  ./schema_test
	# - valgrind --leak-check=<summary>  ./dataframe_test
	# - valgrind --leak-check=<summary>  ./serial_test
	# - valgrind --leak-check=<summary>  ./kvs_test
	# - valgrind --leak-check=<summary>  ./demo_app
	# - valgrind --leak-check=<summary>  ./trivial_app



testdemo:
	- g++ -std=c++11 ./application/demo.cpp -o demo_app
	- ./demo_app 0 8000 3
	- rm demo_app
