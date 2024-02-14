CFLAGS = -g -c -fPIC --std=c++17 -I../argparser -I../stumbler -I/usr/lib/x86_64-linux-gnu/openmpi/include

hubfit: hubfit.o argparser.o stumbler.o
	mpicxx -o hubfit hubfit.o argparser.o stumbler.o -lgsl -lm -lfmt

hubfit.o: hubfit.cc
	mpicxx $(CFLAGS) -o hubfit.o hubfit.cc

argparser.o: ../argparser/argparser.cc
	mpicxx $(CFLAGS) -o argparser.o ../argparser/argparser.cc

stumbler.o: ../stumbler/stumbler.cc
	mpicxx $(CFLAGS) -o stumbler.o ../stumbler/stumbler.cc

clean:
	rm hubfit hubfit.o argparser.o stumbler.o
