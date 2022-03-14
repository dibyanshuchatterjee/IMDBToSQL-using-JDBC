package edu.rit.ibd.a1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class IMDBToSQL {
    private static final String NULL_STR = "\\N";

    public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String folderToIMDBGZipFiles = args[3];
        Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
        final String Movie = " CREATE TABLE Movie"
                + "("
                + " id int(50) not Null,"
                + " ptitle varchar(500),"
                + "otitle varchar(500),"
                + "Adult int(50),"
                + "Year int(50),"
                + "rating float,"
                + "totalVotes int(50),"
                + "runtime int(50),"
                + " PRIMARY KEY (id)"
                + ")";
        final String Genre = "CREATE TABLE Genre"
                + "("
                + "id int(50) not Null,"
                + "name varchar(50),"
                + "PRIMARY KEY (id)"
                + ")";
        final String MovieGenre = "CREATE TABLE MovieGenre" +
                "(" +
                "mid Integer(50)," +
                "gid Integer(50)" +
                ")";
        final String Person = "CREATE TABLE Person" +
                "(" +
                "id int not Null," +
                "Name Varchar(256)," +
                "byear int," +
                "dyear int," +
                "PRIMARY KEY (id)" +
                ")";
        final String Actor = "CREATE TABLE Actor" +
                "(" +
                "mid int(50) not Null," +
                "pid int(50) not Null," +
                "PRIMARY KEY (mid,pid)"+
                ")";
        final String Director = "CREATE TABLE Director" +
                "(" +
                "mid int(50) not Null," +
                "pid int(50) not Null," +
                "PRIMARY KEY (mid,pid)"+
                ")";
        final String Writer = "CREATE TABLE Writer" +
                "(" +
                "mid int(50) not Null," +
                "pid int(50)  not Null," +
                "PRIMARY KEY (mid,pid)"+
                ")";
        final String Producer = "CREATE TABLE Producer" +
                "(" +
                "mid int(50)," +
                "pid int(50)," +
                "PRIMARY KEY (mid,pid)"+
                ")";
        final String KnwonFor = "CREATE TABLE KnownFor" +
                "(" +
                "mid int(50)," +
                "pid int(50)," +
                "PRIMARY KEY (mid,pid)"+
                ")";
        con.setAutoCommit(false);
        PreparedStatement st = con.prepareStatement(Movie);
        st.execute();
        PreparedStatement forGenre = con.prepareStatement(Genre);
        forGenre.execute();
        PreparedStatement MG = con.prepareStatement(MovieGenre);
        MG.execute();
        PreparedStatement forPerson = con.prepareStatement(Person);
        forPerson.execute();
        //creating the other mapping tables
        PreparedStatement actor = con.prepareStatement(Actor);
        PreparedStatement director = con.prepareStatement(Director);
        PreparedStatement knownFor = con.prepareStatement(KnwonFor);
        PreparedStatement writer = con.prepareStatement(Writer);
        PreparedStatement producer = con.prepareStatement(Producer);
        actor.execute();
        director.execute();
        knownFor.execute();
        writer.execute();
        producer.execute();
        int step = 100;
        int cnt = 0;
        int forInsideLoop = 0;//will be sued for rating coloumns
        // Load movies.
        InputStream gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.basics.tsv.gz"));
        Scanner sc = new Scanner(gzipStream, "UTF-8");
        InputStream gzipStream1 = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.ratings.tsv.gz"));
        Scanner sc1 = new Scanner(gzipStream1, "UTF-8"); //for ratings and vote file
        InputStream gzipStream2 = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "name.basics.tsv.gz"));
        Scanner sc2 = new Scanner(gzipStream2, "UTF-8"); //for person table
        InputStream gzipStream3 = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.principals.tsv.gz"));
        Scanner sc3 = new Scanner(gzipStream3, "UTF-8"); //for Actor/Director.... table
        InputStream gzipStream4 = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles + "title.crew.tsv.gz"));
        Scanner sc4 = new Scanner(gzipStream4, "UTF-8"); //for Director/Writer extra
        //populating the Movie table
        st = con.prepareStatement("INSERT INTO Movie(id, ptitle, otitle, Adult, Year, rating, totalVotes, runtime) VALUES(?,?,?,?,?,?,?,?)");
        //insert statement for person table
        forPerson = con.prepareStatement("INSERT INTO Person(id, name, byear, dyear) VALUES(?,?,?,?)");
        String year = "";
        String runT = "";
        Set<String> Genre_List = new HashSet<>();
        Map<Integer, String[]> MovieGenreMap = new HashMap<>();
        //the map to store for the last few tables
        Set<Integer> MovieSet = new HashSet<>();
        //create the Genre_List and associate each Genre with an index
        //create the Movie_id_dictioanry and check what are the genres and check their corresponding indexes
        while (sc.hasNextLine()) {
            cnt++;
            String line = sc.nextLine();
            if (cnt > 1) {
                // Split the line.
                String[] splitLine = line.split("\t");
                if (splitLine[1].equals("movie")) {
                    //entering just the movie values
                    String Id = splitLine[0];
                    String adult = splitLine[4];
                    if (splitLine[5].equals(NULL_STR)) {
                        st.setNull(5, Types.VARCHAR);
                    } else {
                        year = splitLine[5];
                    }

                    if (splitLine[7].equals(NULL_STR)) {
                        st.setNull(8, Types.VARCHAR);
                    } else {
                        runT = splitLine[7];
                    }
                    String Genrename = splitLine[8];
                    //splitting the genre names by comma.
                    String[] Genrename_to_List = Genrename.split(",");
                    //adding all the genre names to the set in each loop.
                    for (int i = 0; i < Genrename_to_List.length; i++) {
                        if (!Genrename_to_List[i].equals(NULL_STR)) {
                            Genre_List.add(Genrename_to_List[i]);
                        }
                    }
                    int IdConverted = Integer.parseInt(Id.substring(2));
                    MovieSet.add(IdConverted);
                    int adultConverted = Integer.parseInt(adult);
                    int yearConverted = Integer.parseInt(year);
                    int runTConverted = Integer.parseInt(runT);
                    MovieGenreMap.put(IdConverted, Genrename.split(","));
                    st.setInt(1, /* Set movie id with appropriate type (integer). */ IdConverted);
                    // Set title and account for null; note the second argument of setNull.
                    if (splitLine[2].equals(NULL_STR))
                        st.setNull(2, Types.VARCHAR);
                    else
                        st.setString(2, splitLine[2]);

                    if (splitLine[3].equals(NULL_STR))
                        st.setNull(3, Types.VARCHAR);
                    else
                        st.setString(3, splitLine[3]);
                    st.setInt(4, adultConverted);
                    st.setInt(5, yearConverted);
                    st.setInt(8, runTConverted);
                    //populating the ratings and votes of Movie Table
                    while (sc1.hasNextLine()) {
                        forInsideLoop++;
                        String line1 = sc1.nextLine();
                        if (forInsideLoop > 1) {
                            //splitting the line
                            String[] splitLine1 = line1.split("\t");
                            String IdforRating = splitLine1[0];
                            String Rating = splitLine1[1];
                            String Votes = splitLine1[2];
                            float RatingConverted = Float.parseFloat(Rating);
                            int VotesConverted = Integer.parseInt(Votes);
                            int IdforRatingConverted = Integer.parseInt(IdforRating.substring(2));
                            if (IdforRatingConverted != IdConverted)
                                continue;
                            else {
                                st.setFloat(6, RatingConverted);
                                st.setInt(7, VotesConverted);
                            }
                            break;
                        }
                    }
                    st.addBatch();
                    if (/* Batch processing */ cnt % step == 0) {
                        st.executeBatch();
                        con.commit();
                    }
                }
            }
        }
        //populating the person table and the known for table
        knownFor = con.prepareStatement("INSERT IGNORE INTO KnownFor(mid,pid) VALUES(?,?)");
        cnt = 0;
        while (sc2.hasNextLine()) {
            cnt++;
            String line2 = sc2.nextLine(); //line to read the name.basics file
            if (cnt > 1) {
                String[] splitLine2 = line2.split("\t");
                forPerson.setInt(1, Integer.parseInt(splitLine2[0].substring(2)));
                forPerson.setString(2, splitLine2[1]);
                if (splitLine2[2].equals(NULL_STR)) {
                    forPerson.setNull(3, Types.VARCHAR);
                } else {
                    forPerson.setInt(3, Integer.parseInt(splitLine2[2]));
                }
                if (splitLine2[3].equals(NULL_STR)) {
                    forPerson.setNull(4, Types.VARCHAR);
                } else {
                    forPerson.setInt(4, Integer.parseInt(splitLine2[3]));
                }
                forPerson.addBatch();
                if (/* Batch processing */ cnt % step == 0) {
                    forPerson.executeBatch();
                    con.commit();
                }
                String[] knownForArray = splitLine2[5].split(",");
                for(int i = 0; i<knownForArray.length; i++){
                    if(!knownForArray[i].equals(NULL_STR)){
                        if (MovieSet.contains(Integer.parseInt(knownForArray[i].substring(2)))){
                               knownFor.setInt(2, Integer.parseInt(splitLine2[0].substring(2)));
                               knownFor.setInt(1, Integer.parseInt(knownForArray[i].substring(2)));
                               knownFor.addBatch();
                        }
                    }
                }
                if (/* Batch processing */ cnt % step == 0) {
                    forPerson.executeBatch();
                    knownFor.executeBatch();
                    con.commit();
                }
            }
        }
        cnt = 0;

        List<String> To_Store_Index = new ArrayList<String>(Genre_List);
        //populating Genre
        forGenre = con.prepareStatement("INSERT INTO Genre(id, name) VALUES(?,?)");
        int step_for_genre = 1000;
        for (int counter = 0; counter < To_Store_Index.size(); counter++) {
            forGenre.setInt(1, counter + 1);
            forGenre.setString(2, To_Store_Index.get(counter));
            forGenre.addBatch();
            if (/* Batch processing */ counter % step_for_genre == 0) {
                forGenre.executeBatch();
                con.commit();
            }

        }
        //for Movie Genre create a dictionary with the key as movie id and value as an array
        //then start working on the MovieGenre
        // Populating Movie Genre
        MG = con.prepareStatement("INSERT INTO MovieGenre(mid, gid) VALUES(?,?)");
        for (Map.Entry<Integer, String[]> entry : MovieGenreMap.entrySet()) {
            cnt++;
            int key = entry.getKey();
            for (int i = 0; i < entry.getValue().length; i++) {
                String value = entry.getValue()[i];
                if (To_Store_Index.indexOf(value) != -1) {
                    MG.setInt(1, key);
                    MG.setInt(2, To_Store_Index.indexOf(value));
                    MG.addBatch();
                }
            }
            if (/* Batch processing */ cnt % step_for_genre == 0) {
                MG.executeBatch();
                con.commit();
            }

        }
        con.commit();
        //staring to populate the last tables
        actor = con.prepareStatement("INSERT IGNORE INTO Actor(mid, pid) VALUES(?,?)");
        director = con.prepareStatement("INSERT IGNORE INTO Director(mid, pid) VALUES(?,?)");
        writer = con.prepareStatement("INSERT IGNORE INTO Writer(mid,pid) VALUES(?,?)");
 //        knownFor = con.prepareStatement("INSERT INTO KnownFor(mid,pid) VALUES(?,?)");
        producer = con.prepareStatement("INSERT IGNORE INTO Producer(mid,pid) VALUES(?,?)");
        cnt = 0;
        while (sc3.hasNextLine()) {
            String line3 = sc3.nextLine();
            cnt++;
            if (cnt > 1) {
                String[] splitLine3 = line3.split("\t");
                if (MovieSet.contains(Integer.parseInt(splitLine3[0].substring(2)))) {
                    if (splitLine3[3].equals("actor") || splitLine3[3].equals("self") || splitLine3[3].equals("actress")) {
                        //start populating actor table
                        actor.setInt(1, Integer.parseInt(splitLine3[0].substring(2)));
                        actor.setInt(2, Integer.parseInt(splitLine3[2].substring(2)));
                        actor.addBatch();
                    }
                    if (splitLine3[3].equals("writer")) {
                        //start populating writer table
                        writer.setInt(1, Integer.parseInt(splitLine3[0].substring(2)));
                        writer.setInt(2, Integer.parseInt(splitLine3[2].substring(2)));
                        writer.addBatch();
                    }
                    if (splitLine3[3].equals("director")) {
                        //start populating director table
                        director.setInt(1, Integer.parseInt(splitLine3[0].substring(2)));
                        director.setInt(2, Integer.parseInt(splitLine3[2].substring(2)));
                        director.addBatch();
                    }
                    if (splitLine3[3].equals("producer")) {
                        //start populating producer table
                        producer.setInt(1, Integer.parseInt(splitLine3[0].substring(2)));
                        producer.setInt(2, Integer.parseInt(splitLine3[2].substring(2)));
                        producer.addBatch();
                    }
                    if (/* Batch processing */ cnt % step == 0) {
                        actor.executeBatch();
                        writer.executeBatch();
                        producer.executeBatch();
                        director.executeBatch();
                        con.commit();
                    }
                }

            }
        }
        actor.executeBatch();
        writer.executeBatch();
        producer.executeBatch();
        director.executeBatch();

        cnt = 0;
        while (sc4.hasNextLine()) {
            String line4 = sc4.nextLine();
            cnt++;
            if (cnt > 1) {
                String[] splitLine4 = line4.split("\t");
                if (MovieSet.contains(Integer.parseInt(splitLine4[0].substring(2)))) {
                    String[] directorStore = splitLine4[1].split(",");
                    String[] writerStore = splitLine4[2].split(",");
                    for (int i = 0; i < directorStore.length; i++) {
                        if (!directorStore[i].equals(NULL_STR)) {
                            director.setInt(1, Integer.parseInt(splitLine4[0].substring(2)));
                            director.setInt(2, Integer.parseInt(directorStore[i].substring(2)));
                            director.addBatch();
                        }
                    }
                    for (int j = 0; j < writerStore.length; j++) {
                        if (!writerStore[j].equals(NULL_STR)) {
                            writer.setInt(1, Integer.parseInt(splitLine4[0].substring(2)));
                            writer.setInt(2, Integer.parseInt(writerStore[j].substring(2)));
                            writer.addBatch();

                        }
                    }

                }

            }
            if (/* Batch processing */ cnt % step == 0) {
                writer.executeBatch();
                director.executeBatch();
                con.commit();
            }
        }
        sc.close();
        sc1.close();
        sc2.close();
        sc3.close();
        sc4.close();
        st.executeBatch();
        forGenre.executeBatch();
        MG.executeBatch();
        forPerson.executeBatch();
        actor.executeBatch();
        writer.executeBatch();
        producer.executeBatch();
        knownFor.executeBatch();
        director.executeBatch();
        con.commit();
        //DELETING FROM THE TABLES WHERE PIDs DON'T EXIST:
        //deleting from actors
        actor = con.prepareStatement("DELETE FROM Actor WHERE pid NOT IN (SELECT id FROM Person)");
        actor.executeUpdate();
        con.commit();
        director = con.prepareStatement("DELETE FROM Director WHERE pid NOT IN (SELECT id FROM Person)");
        director.executeUpdate();
        con.commit();
        writer = con.prepareStatement("DELETE FROM Writer WHERE pid NOT IN (SELECT id FROM Person)");
        writer.executeUpdate();
        con.commit();
        producer = con.prepareStatement("DELETE FROM Producer WHERE pid NOT IN (SELECT id FROM Person)");
        producer.executeUpdate();
        con.commit();
        knownFor = con.prepareStatement("DELETE FROM KnownFor WHERE pid NOT IN (SELECT id FROM Person)");
        knownFor.executeUpdate();
        con.commit();

        con.commit();
        st.close();
        forGenre.close();
        MG.close();
        forPerson.close();
        actor.close();
        writer.close();
        producer.close();
        knownFor.close();
        director.close();
        con.close();

    }

}
