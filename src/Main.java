import java.sql.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","system","Polsat131");

        Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = st.executeQuery("Select * from PRACOWNICY");
        while(rs.next()){
            String nazwisko = rs.getString("NAZWISKO");
            float placa = rs.getFloat("PLACA_POD");
            System.out.println(nazwisko + " " + placa);
        }
        rs = null;
        //zadanie 1
        rs = st.executeQuery("Select (Select count(*) from PRACOWNICY p where p.ID_ZESP = z.ID_ZESP), z.NAZWA from ZESPOLY z ");
        while(rs.next()){
            System.out.println(rs.getInt(1) + " jest zatrudnionych w " + rs.getString(2));
        }
        rs = null;
        //zadanie 2
        rs = st.executeQuery("select PRACOWNICY.NAZWISKO from PRACOWNICY where ETAT = 'ASYSTENT' order by PLACA_POD desc ");
        System.out.println(rs.absolute(-3) + ": " + rs.getString(1));
        System.out.println(rs.relative(1) + ": " + rs.getString(1));
        System.out.println(rs.relative(1) + ": " + rs.getString(1));
        rs = null;
        //zadanie 3
        conn.setAutoCommit(false);
        rs = st.executeQuery("select nazwa from ETATY");
        System.out.println("przed rollbackiem");
        while(rs.next()){
            System.out.println(rs.getString(1));
            System.out.println(rs.toString());
        }
        //st.executeUpdate("insert  into etaty(nazwa, placa_min, placa_max) values ('nowy', 100, 300)");
        rs = null;
        rs = st.executeQuery("select ETATY.NAZWA from ETATY");
        while(rs.next()){
            System.out.println(rs.getString(1));
        }
        conn.rollback();
        rs = st.executeQuery("select ETATY.NAZWA from ETATY");
        System.out.println("Po rollbacku");
        while(rs.next()){
            System.out.println(rs.getString(1));
        }
       // st.executeUpdate("insert into ETATY(nazwa, placa_min, placa_max) values ('REKTOR', 1000, 3000)");
        conn.commit();
        rs = st.executeQuery("select * from ETATY");
        while(rs.next()){
            System.out.println(rs.getString(1) + " " + rs.getInt(2) + " " + rs.getInt(3));
        }
        //zadanie 5
        String nazwiska[] = {"Woźniak","Dąbrowski","Kozłowski"};
        int place[] = {1300,1700,1500};
        String etaty[] = {"ASYSTENT", "PROFESOR","ADIUNKT"};
        PreparedStatement prst = conn.prepareStatement("insert  into PRACOWNICY(ID_PRAC,NAZwisko, ETAT, PLACA_POD)" +
                "values ((Select MAX(ID_PRAC) + 10 from PRACOWNICY),?,?,?)");
        for(int i = 0; i <3; i++ ) {
            prst.setString(1, nazwiska[i]);
            prst.setInt(3, place[i]);
            prst.setString(2,etaty[i]);
            prst.executeUpdate();
        }
        rs = st.executeQuery("SELECT * from PRACOWNICY");
        while(rs.next()){
            System.out.println(rs.getInt(1) + ": " + rs.getString(2));
        }
        conn.rollback();
        //zadanie 6
        prst = conn.prepareStatement("insert  into PRACOWNICY(ID_PRAC,NAZwisko, ETAT, PLACA_POD)" +
                "values (?,?,?,?)");
        for(int i =0 ; i < 2000; i++){
            prst.setInt(1, 270 + 10* i);
            prst.setString(2, "zad6");
            prst.setInt(4, 1000 + i );
            prst.setString(3, etaty[2]);
            prst.addBatch();
        }
        long timeStart = System.nanoTime();
        System.out.println(prst.executeBatch());
        long timeEnd = System.nanoTime();
        System.out.println("Dodano za pomocą komendy batch w: " + (timeEnd - timeStart));
        conn.rollback();
        prst = conn.prepareStatement("insert  into PRACOWNICY(ID_PRAC,NAZwisko, ETAT, PLACA_POD)" +
                "values ((Select MAX(ID_PRAC) + 10 from PRACOWNICY),?,?,?)");
        timeStart=System.nanoTime();
        for(int i = 0 ; i < 2000; i++){
            prst.setInt(3, 1000+i);
            prst.setString(1, "zad6b");
            prst.setString(2, etaty[1]);
            prst.executeUpdate();
        }
        System.out.println("Dodano iteracyjnie 2000 rekordów w: " +(System.nanoTime() - timeStart));
        conn.rollback();
        rs.close();
        st.close();
        conn.close();
    }
}
