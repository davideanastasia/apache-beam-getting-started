bootstrap:
	test -f data/input/shakespeare.txt || ./bootstrap.sh

clean-output:
	rm -fr data/indexer/

run: clean-output
	java -cp target/apache-beam-getting-started-bundled-1.0-SNAPSHOT.jar com.davideanastasia.beam.gs.Indexer
