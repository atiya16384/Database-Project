import java.sql.*;
import java.io.File;
import  java.io.Reader;
import java.io.BufferedReader;
import  java.io.FileReader;
import java.util.*;
public class Connect {
  
//The program runs with the following command: java -classpath ".:sqlite-jdbc-3.20.1.jar" Connect.java
  public static void main( String args[] ) {
    
      Connection conn=null;
    
      String filePath="Customers.csv";
      String filePath1="Items.csv";
      String filePath2="Combat.csv";
      int batchSize=20;
      int batchSize2=20;
      int batchSize3=20;
      Statement stmnt5 = null;
      
      try{
         String url="jdbc:sqlite:TRY.db";
         conn=DriverManager.getConnection(url);
         System.out.println("Connection to SQLite has been established");
         
         Statement stmnt = null;

      	 stmnt= conn.createStatement();
      	 String sql = "CREATE TABLE Player" +
         "(Account_Number INTEGER NOT NULL," +
         "Forename VARCHAR," +
         "Surname VARCHAR ," +
         "Email_Address VARCHAR," +
         "PRIMARY KEY (Account_Number))";   

        stmnt.executeUpdate(sql);
     
        stmnt.close();
        conn.setAutoCommit(false);
        String sqlInsert="INSERT OR IGNORE INTO Player(Account_Number, Forename, Surname, Email_Address)" + "VALUES(?,?,?,?)";
       
        PreparedStatement statement = conn.prepareStatement(sqlInsert);
      //  conn.createStatement().execute("PRAGMA foreign_key=ON");
        BufferedReader lineReader = new BufferedReader(new FileReader(filePath));
        String lineText=null;
        int count =0;
        lineReader.readLine();
        while((lineText=lineReader.readLine())!=null){
        	String[] data= lineText.split(",");
        	
        	String Account_Number = data[0];
        	String Forename=data[1];
        	String Surname= data[2];
        	String Email_Address=data[3];

        	statement.setString(1,Account_Number);
        	statement.setString(2, Forename);
        	statement.setString(3, Surname);
        	statement.setString(4, Email_Address);
        	statement.addBatch();
        try{
        	statement.executeBatch();
        	if( count % batchSize==0){
        		statement.executeBatch();
        		System.out.println("Data has been inserted successfully");
 }

      } 
      
         catch(SQLIntegrityConstraintViolationException e){
      	//some if statement, like if 
      	System.err.println("WARNING");
      	}
}
      lineReader.close();
      //execute the remaining queries
      statement.executeBatch();
      conn.commit();
      conn.close();

    }  catch(Exception exception){
         exception.printStackTrace();
      }
      
      try{
         String url2="jdbc:sqlite:TRY.db";
         conn=DriverManager.getConnection(url2);
         System.out.println("Connection to SQLite has been established");
         
         Statement stmnt2 = null;
            //Account_Number in customers.csv has the same value repeated twice

            stmnt2= conn.createStatement();
            String sql2 = "CREATE TABLE Character_has" +
         "(Account_Number INTEGER NOT NULL," +
         "Creation_Date VARCHAR," +
         "Expiry_Date VARCHAR,"+
         "Character_Name VARCHAR NOT NULL,"+
         "Character_Type VARCHAR,"+
         "Level INTEGER,"+
         "ExperiencePoints INTEGER,"+
         "MaxHealth INTEGER,"+
         "Health INTEGER,"+
         "AttackinScore INTEGER,"+
         "DefenceScore INTEGER,"+
         "ManaScore VARCHAR,"+
         "Money_bank REAL,"+
         "PRIMARY KEY (Character_Name, Account_Number)"+
         "FOREIGN KEY(Account_Number) REFERENCES Player(Account_Number) ON DELETE CASCADE)";   

         stmnt2.executeUpdate(sql2);
         stmnt2.close();
         conn.setAutoCommit(false);

         String sqlInsert2="INSERT OR IGNORE INTO Character_has(Account_Number, Creation_Date, Expiry_Date, Character_Name,  Character_Type, Level, ExperiencePoints, MaxHealth, Health, AttackInScore, DefenceScore, ManaScore, Money_bank)" + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
      //   how to ignore empty string in csv
         
         PreparedStatement statement2 = conn.prepareStatement(sqlInsert2);
         conn.createStatement().execute("PRAGMA foreign_key=ON");
         BufferedReader lineReader2 = new BufferedReader(new FileReader(filePath));
         String lineText2=null;
         int count2 =0;
         lineReader2.readLine();
         while((lineText2=lineReader2.readLine())!=null){
         String[] data2= lineText2.split(",");
         
         String Account_Number2 = data2[0];
         String Creation_Date2 = data2[4];
         String Expiry_Date2= data2[5];
         String Character_Name2 = data2[6];
         String Character_Type2= data2[7];
         String Level2= data2[8];
         String ExperiencePoints2= data2[9];
         String MaxHealth2= data2[10];
         String Health2= data2[11];
         String AttackinScore2= data2[12];
         String DefenceScore2= data2[13];
         String ManaScore2= data2[15];
         String Money_bank2=data2[16];

         statement2.setString(1,Account_Number2);
         statement2.setString(2, Creation_Date2);
         statement2.setString(3, Expiry_Date2);
         statement2.setString(4, Character_Name2);
         statement2.setString(5, Character_Type2);
         statement2.setString(6,ExperiencePoints2);
         statement2.setString(7, Level2);
         statement2.setString(8, MaxHealth2);
         statement2.setString(9,Health2);
         statement2.setString(10, AttackinScore2);
         statement2.setString(11, DefenceScore2);
         statement2.setString(12, ManaScore2);
         statement2.setString(13, Money_bank2);

         statement2.addBatch();

         try{
         statement2.executeBatch();
         if( count2 % batchSize2==0){
            statement2.executeBatch();
            System.out.println("Data has been inserted successfully");
   }

      } 
      
         catch(SQLIntegrityConstraintViolationException e2){
         //some if statement, like if 
         System.err.println("WARNING");
         }
   }
      lineReader2.close();
      //execute the remaining queries
      statement2.executeBatch();
      conn.commit();
      conn.close();

      }  catch(Exception exception2){
         exception2.printStackTrace();
      }
      //how to ignore certain columns from the csv file
      
    //how to ignore importing certain columns from the table
    
    try{
      String url3="jdbc:sqlite:TRY.db";
      conn=DriverManager.getConnection(url3);
      System.out.println("Connection to SQLite has been established");
      
      Statement stmnt3 = null;
         //Account_Number in customers.csv has the same value repeated twice

       stmnt3= conn.createStatement();
       String sql3 = "CREATE TABLE Item" +
      "(ItemName VARCHAR NOT NULL," +
      "Price REAL," +
      "Quantity INTEGER ," +
      "HealingScore INTEGER,"+
      "ManaScore INTEGER," +
      "PRIMARY KEY (ItemName))";   

     stmnt3.executeUpdate(sql3);
  
     stmnt3.close();
     conn.setAutoCommit(false);
     String sqlInsert3="INSERT OR IGNORE INTO Item(ItemName, Price, Quantity, HealingScore, ManaScore)" + "VALUES(?,?,?,?,?)";
    
     PreparedStatement statement3 = conn.prepareStatement(sqlInsert3);
     conn.createStatement().execute("PRAGMA foreign_key=ON");
     BufferedReader lineReader3 = new BufferedReader(new FileReader(filePath1));
     String lineText3=null;
     int count3 = 0;
     lineReader3.readLine();
     while(((lineText3 = lineReader3.readLine())!=null)){
        String[] data3 = lineText3.split(",");
        
        String ItemName3 =data3[1];
        String Price3=data3[5];
        String Quantity3= data3[6];
        String HealingScore3=data3[9];
        String ManaScore3=data3[10];
       // String Equipped3=data3[15];

        statement3.setString(1, ItemName3);
        statement3.setString(2, Price3);
        statement3.setString(3, Quantity3);
        statement3.setString(4, HealingScore3);
        statement3.setString(5, ManaScore3);
      //  statement3.setString(6, Equipped3);

        statement3.addBatch();
     try{
        statement3.executeBatch();
        if( count3 % batchSize3==0){
           statement3.executeBatch();
           System.out.println("Data has been inserted successfully");
}

   } 
   
      catch(SQLIntegrityConstraintViolationException e3){
      //some if statement, like if 
      System.err.println("WARNING");
      }
}
   lineReader3.close();
   //execute the remaining queries
   statement3.executeBatch();
   conn.commit();
   conn.close();

 }  catch(Exception exception3){
      exception3.printStackTrace();
   }


   try{
      String url4="jdbc:sqlite:TRY.db";
      conn=DriverManager.getConnection(url4);
      System.out.println("Connection to SQLite has been established");
      
      Statement stmnt4 = null;
         //Account_Number in customers.csv has the same value repeated twice

       stmnt4= conn.createStatement();
       String sql4 = "CREATE TABLE possessItem" +
      "(ItemName VARCHAR NOT NULL," +
      "Price REAL," +
      "Quantity INTEGER," + 
      "HealingScore INTEGER," +
      "ManaScore VARCHAR," +
      "Account_Number INTEGER ,"+
      "Character_Name VARCHAR,"+
      "PRIMARY KEY (ItemName),"+
      "FOREIGN KEY(Account_Number) REFERENCES Character_has(Account_Number) ON DELETE CASCADE,"+
      "FOREIGN KEY(Character_Name) REFERENCES Character_has(Character_Name) ON DELETE CASCADE )";   

     stmnt4.executeUpdate(sql4);
  
     stmnt4.close();
     conn.setAutoCommit(false);
     String sqlInsert4="INSERT OR IGNORE INTO possessItem(ItemName, Price, Quantity, ManaScore, HealingScore, Account_Number, Character_Name)" + "VALUES(?,?,?,?,?,?,?)";
    
     PreparedStatement statement4 = conn.prepareStatement(sqlInsert4);
     conn.createStatement().execute("PRAGMA foreign_key=ON");
     
     BufferedReader lineReader4 = new BufferedReader(new FileReader(filePath1));
  
     String lineText4=null;

     int count4 =0;
   
     lineReader4.readLine();
    // lineText123.readLine();
     while(((lineText4=lineReader4.readLine())!=null )){
        String[] data4= lineText4.split(",");
     //   String[] data123 = lineReader4.split(",");
        String Name4 = data4[1];
        String Price4=data4[5];
        String Quantity4= data4[6];
        String HealingScore4=data4[9];
        String ManaScore4=data4[10];

      

        statement4.setString(1,Name4);
        statement4.setString(2, Price4);
        statement4.setString(3, Quantity4);
        statement4.setString(4, ManaScore4);
        statement4.setString(5, HealingScore4);

 
        statement4.addBatch();
        
     try{
        statement4.executeBatch();
        if( count4 % batchSize==0){
           statement4.executeBatch();
           System.out.println("Data has been inserted successfully");
}
   } 
      catch(SQLIntegrityConstraintViolationException e4){
      //some if statement, like if 
      System.err.println("WARNING");
      }
}
   lineReader4.close();
   //execute the remaining queries
   statement4.executeBatch();
   conn.commit();
   conn.close();
//return connect;
 }  catch(Exception exception4){
      exception4.printStackTrace();
    //  return null;
   }

   try{
      String url5="jdbc:sqlite:TRY.db";
      conn=DriverManager.getConnection(url5);
      System.out.println("Connection to SQLite has been established");

         //Account_Number in customers.csv has the same value repeated twice

       stmnt5= conn.createStatement();
       String sql5 = "CREATE TABLE CombatActivities" +
      "(Battle_Date VARCHAR,"+
      "BattleNo INTEGER NOT NULL," +
      "Attacker VARCHAR,"+
      "Defender VARCHAR ,"+
      "Result Integer," +
      "Damage Integer ," +
      "PRIMARY KEY (BattleNo))";   

     stmnt5.executeUpdate(sql5);
  
     stmnt5.close();
     conn.setAutoCommit(false);
     String sqlInsert5="INSERT OR IGNORE INTO CombatActivities(Battle_Date, BattleNo, Attacker, Defender, Result, Damage )" + "VALUES(?,?,?,?,?,?)";
    
     PreparedStatement statement5 = conn.prepareStatement(sqlInsert5);
     conn.createStatement().execute("PRAGMA foreign_key=ON");
     
     BufferedReader lineReader5 = new BufferedReader(new FileReader(filePath2));
     String lineText5=null;
     int count5 =0;
     lineReader5.readLine();
     while((lineText5=lineReader5.readLine())!=null){
        String[] data5= lineText5.split(",");
        
        String Battle_Date5 = data5[0];
        String BattleNo5=data5[1];
        String Attacker5 = data5[2];
        String Defender5=data5[3];
        String Result5= data5[5];
        String Damage5=data5[6];

        statement5.setString(1, Battle_Date5);
        statement5.setString(2, BattleNo5);
        statement5.setString(3, Attacker5);
        statement5.setString(4, Defender5);
        statement5.setString(5, Result5);
        statement5.setString(6, Damage5);
        statement5.addBatch();
   
     try{
        statement5.executeBatch();
        if( count5 % batchSize==0){
           statement5.executeBatch();
           System.out.println("Data has been inserted successfully");
}

   } 
   
      catch(SQLIntegrityConstraintViolationException e5){
      //some if statement, like if 
      System.err.println("WARNING");
      }
}
      
   lineReader5.close();
   //execute the remaining queries
   statement5.executeBatch();
   conn.commit();
   conn.close();

 }  catch(Exception exception5){
      exception5.printStackTrace();
   }
   
   try{
      String url6="jdbc:sqlite:TRY.db";
      conn=DriverManager.getConnection(url6);
      System.out.println("Connection to SQLite has been established");
      
      Statement stmnt6 = null;

       stmnt6= conn.createStatement();
       String sql6 = "CREATE TABLE engagesInCombat" +
      "(Account_Number INTEGER(8) NOT NULL ," +
      "Character_Name VARCHAR NOT NULL,"+
      "BattleNo INTEGER NOT NULL," +
      "PRIMARY KEY (Account_Number, Character_Name, BattleNo),"+
      "FOREIGN KEY(Account_Number) REFERENCES Character_has (Account_Number) ON DELETE CASCADE," +
      "FOREIGN KEY(Character_Name) REFERENCES Character_has (Character_Name) ON DELETE CASCADE," +
      "FOREIGN KEY(BattleNo) REFERENCES CombatActivities(BattleNo) ON DELETE CASCADE)";
    

     stmnt6.executeUpdate(sql6);
  
     stmnt6.close();
     conn.setAutoCommit(false);
     String sqlInsert6="INSERT OR IGNORE INTO engagesInCombat(Account_Number, Character_Name, BattleNo)" + "VALUES(?,?,?)";
    
     PreparedStatement statement6 = conn.prepareStatement(sqlInsert6);
   //  conn.createStatement().execute("PRAGMA foreign_key=ON");
     BufferedReader lineReader6 = new BufferedReader(new FileReader(filePath2));
     String lineText6=null;
     int count6 =0;
     lineReader6.readLine();
     while((lineText6=lineReader6.readLine())!=null){
        String[] data6= lineText6.split(",");
        
       
        statement6.addBatch();
     try{
        statement6.executeBatch();
        if( count6 % batchSize==0){
           statement6.executeBatch();
           System.out.println("Data has been inserted successfully");
}

   } 
   
      catch(SQLIntegrityConstraintViolationException e6){
      //some if statement, like if 
      System.err.println("WARNING");
      }
}
   lineReader6.close();
   //execute the remaining queries
   statement6.executeBatch();
   conn.commit();
   conn.close();

 }  catch(Exception exception6){
      exception6.printStackTrace();

   }
   try{
      String url7="jdbc:sqlite:TRY.db";
      conn=DriverManager.getConnection(url7);
      System.out.println("Connection to SQLite has been established");
      
      Statement stmnt7 = null;

       stmnt7= conn.createStatement();
       String sql7 = "CREATE TABLE ItemEngagesInCombat" +
      "(ItemName VARCHAR NOT NULL," +
      "BattleNo Integer NOT NULL," +
      "PRIMARY KEY (BattleNo, ItemName),"+
      "FOREIGN KEY(ItemName) REFERENCES Item(ItemName) ON DELETE CASCADE,"+
      "FOREIGN KEY(BattleNo) REFERENCES CombatActivities(BattleNo) ON DELETE CASCADE)";   

     stmnt7.executeUpdate(sql7);
   
     stmnt7.close();
     conn.setAutoCommit(false);
     String sqlInsert7="INSERT OR IGNORE INTO ItemEngagesInCombat(ItemName, BattleNo)" + "VALUES(?,?)";
     PreparedStatement statement7 = conn.prepareStatement(sqlInsert7);

   // //execute the remaining queries

   statement7.executeBatch();
   conn.commit();
   conn.close();

 }  catch(Exception exception7){
      exception7.printStackTrace();
   }

   try{
      String url8="jdbc:sqlite:TRY.db";
      conn=DriverManager.getConnection(url8);
      System.out.println("Connection to SQLite has been established");
      
      Statement stmnt8 = null;

       stmnt8= conn.createStatement();
       String sql8 = "CREATE TABLE Armour" +
      "(ItemName VARCHAR ," +
      "DefendScore INTEGER," +
      "BodyPart VARCHAR," +
      "PRIMARY KEY(ItemName),"+
      "FOREIGN KEY(ItemName) REFERENCES Item(ItemName) ON DELETE CASCADE)";   

     stmnt8.executeUpdate(sql8);
  
     stmnt8.close();
     conn.setAutoCommit(false);
     String sqlInsert8="INSERT OR IGNORE INTO Armour(ItemName, DefendScore, BodyPart)" + "VALUES(?,?,?)";
    
     PreparedStatement statement8 = conn.prepareStatement(sqlInsert8);
   //  conn.createStatement().execute("PRAGMA foreign_key=ON");
     BufferedReader lineReader8 = new BufferedReader(new FileReader(filePath1));
     String lineText8=null;
     int count8 =0;
     lineReader8.readLine();
     while((lineText8=lineReader8.readLine())!=null){
        String[] data8= lineText8.split(",");
        
        String DefendScore8=data8[7];
        String BodyPart8= data8[14];
        

        statement8.setString(2, DefendScore8);
        statement8.setString(3, BodyPart8);
        statement8.addBatch();
     try{
        statement8.executeBatch();
        if( count8 % batchSize==0){
           statement8.executeBatch();
           System.out.println("Data has been inserted successfully");
}

   } 
   
      catch(SQLIntegrityConstraintViolationException e8){
      //some if statement, like if 
      System.err.println("WARNING");
      }
}
   lineReader8.close();
   //execute the remaining queries
   statement8.executeBatch();
   conn.commit();
   conn.close();

 }  catch(Exception exception8){
      exception8.printStackTrace();
   }
   
   try{
      String url9="jdbc:sqlite:TRY.db";
      conn=DriverManager.getConnection(url9);
      System.out.println("Connection to SQLite has been established");
      
      Statement stmnt9 = null;

       stmnt9= conn.createStatement();
       String sql9 = "CREATE TABLE Weapon" +
      "(ItemName VARCHAR," +
      "AttackScore INTEGER," +
      "BodyPart VARCHAR," +
      "PRIMARY KEY (ItemName),"+
      "FOREIGN KEY (ItemName) REFERENCES Item(ItemName) ON DELETE CASCADE)";   

     stmnt9.executeUpdate(sql9);
  
     stmnt9.close();
     conn.setAutoCommit(false);
     String sqlInsert9="INSERT OR IGNORE INTO Weapon(ItemName, AttackScore, BodyPart)" + "VALUES(?,?,?)";
    
     PreparedStatement statement9 = conn.prepareStatement(sqlInsert9);
  //   conn.createStatement().execute("PRAGMA foreign_key=ON");
     BufferedReader lineReader9 = new BufferedReader(new FileReader(filePath1));
     String lineText9=null;
     int count9 =0;
     lineReader9.readLine();
     while((lineText9=lineReader9.readLine())!=null){
        String[] data9= lineText9.split(",");
        String AttackScore9=data9[8];
        String BodyPart9= data9[14];

        statement9.setString(2, AttackScore9);
        statement9.setString(3, BodyPart9);
        statement9.addBatch();
     try{
        statement9.executeBatch();
        if( count9 % batchSize==0){
           statement9.executeBatch();
           System.out.println("Data has been inserted successfully");
}

   } 
      catch(SQLIntegrityConstraintViolationException e9){
      //some if statement, like if 
      System.err.println("WARNING");
      }
}
   lineReader9.close();
   //execute the remaining queries
   statement9.executeBatch();
   conn.commit();
   conn.close();

 }  catch(Exception exception9){
      exception9.printStackTrace();
   }

   try {
     // conn = this.connect();

            Statement stmt5 = conn.createStatement();

            // 1. Top 5 characters with the highest number of successful combats attacks.
            ResultSet rs1 = stmt5.executeQuery("SELECT Attacker, " +
                    "COUNT(*) as SuccessfulAttacks " +
                    "FROM CombatActivities WHERE Result = 'Victory' " +
                    "GROUP BY Attacker ORDER BY successful_attacks DESC LIMIT 5");
            System.out.println("Top 5 characters with the highest number of successful combat attacks:");
            while (rs1.next()) {
                System.out.println(rs1.getString("Attacker") + " - " + rs1.getInt("Successful_Attacks"));
            }
              // 2. Total number of attacks per character having more than 5 attacks.
            ResultSet rs2 = stmt5.executeQuery("SELECT Attacker, " +
              "COUNT(*) as TotalAttacks " +
              "FROM CombatActivities GROUP BY Attacker " +
              "HAVING TotalAttacks > 5");
            System.out.println("\nCharacters with more than 5 attacks:");
            while (rs2.next()) {
          System.out.println(rs2.getString("Attacker") + " - " + rs2.getInt("TotalAttacks"));
      }

      // 3. Order the names of characters from highest to lowest number of attacks.
      ResultSet rs3 = stmt5.executeQuery("SELECT Attacker, " +
              "COUNT(*) as TotalAttacks " +
              "FROM CombatActivities GROUP BY Attacker " +
              "ORDER BY TotalAttacks DESC");
      System.out.println("\nCharacters ordered by number of attacks (highest to lowest):");
      while (rs3.next()) {
          System.out.println(rs3.getString("Attacker") + " - " + rs3.getInt("TotalAttacks"));
      }

      // 4.Players with at least 5 characters.
      ResultSet rs4 = stmt5.executeQuery("SELECT Account_Number, " +
              "COUNT(*) as TotalCharacters + FROM Character_has " +
              "GROUP BY Account_number HAVING TotalCharacters >= 5");
      System.out.println("\nPlayers with at least 5 characters:");
      while (rs4.next()) {
          System.out.println("Account Number: " + rs4.getInt("Account_Number") + " - Total Characters: " + rs4.getInt("TotalCharacters"));
      }
      
   
} catch (SQLException e) {
   System.out.println(e.getMessage());
} finally {
   try {
       if (conn != null) {
           conn.close();
       }
   } catch (SQLException ex) {
       System.out.println(ex.getMessage());
   }
}
   }

}
      

