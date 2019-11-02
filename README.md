# IMDB Title ranking exercise


    Usage: run.sh [options]
    
      -m, --minVotes <value>   Minimum number of votes for a title (default: 50)
      -t, --topTitles <value>  Number of top titles to selec (default: 20)
      --sparkMaster <value>    Spark Master host (default: local[*])
      --timeout <value>        Spark job timeout (default 1 hour)
      --titleRatingsFile <file>
                               File containing the ratings information for titles (title.ratings.tsv.gz)
      --titleAkasFile <file>   File containing the title names (title.akas.tsv.gz)
      --titlePrincipalsFile <file>
                               File containing title principals (title.principals.tsv.gz)
      --nameBasicsFile <file>  File containing information for names (name.basics.tsv.gz )