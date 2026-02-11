import src.etl.Source as source
import src.etl.Injest as ingest
import src.etl.Transform as transform



def main():
    # Create Source
    # src = source.Source()
    # src.generate()

    # Injest data
    Instngest = ingest.Injest()
    Instngest.run()

    

    

main()