extern void test_libversion();
extern void test_libversion_number();
extern void test_open_misuse();
extern void test_open_not_found();
extern void test_open_existing();
extern void test_close();
extern void test_prepare_misuse();

int main(int argc, char *argv[])
{
	test_libversion();
	test_libversion_number();
	test_open_misuse();
	test_open_not_found();
	test_open_existing();
	test_close();
	test_prepare_misuse();

	return 0;
}
