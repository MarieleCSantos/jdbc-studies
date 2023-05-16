package application;

import db.DB;
import db.DbException;
import db.DbIntegrityException;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Program {
    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        Connection connection = null;
        Statement st = null;
        ResultSet rs = null;

        //recupera dados do banco
        try {
            connection = DB.getConnection();

            st = connection.createStatement();
            rs = st.executeQuery("select * from department");

            while (rs.next()) {
                System.out.println(rs.getInt("Id") + ", " + rs.getString("Name"));
            }

        } catch (SQLException e) {
            e.printStackTrace();

        } finally {
//            DB.closeResultSet(rs);
//            DB.closeStatement(st);
//            DB.closeConnection();
        }

        //insere dados no banco
        PreparedStatement ps = null;
        try {
            connection = DB.getConnection();

            ps = connection.prepareStatement(
                    "INSERT INTO seller " +
                            "(Name, Email, BirthDate, BaseSalary, DepartmentId)" +
                            "VALUES " +
                            "(?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);

            ps.setString(1, "Carl Purple");
            ps.setString(2, "carl@gmail.com");
            ps.setDate(3, new Date(sdf.parse("22/04/1985").getTime()));
            ps.setDouble(4, 3000.0);
            ps.setInt(5, 4);


            //podemos tambem adicionar mais de um valor por vez, como na proxima linha:
            ps = connection.prepareStatement("INSERT INTO department (Name) values ('D1'), ('D2')",
                    Statement.RETURN_GENERATED_KEYS);

            int rowsAffected = ps.executeUpdate();

            if (rowsAffected > 0) {
                ResultSet rst = ps.getGeneratedKeys();
                while (rst.next()) {
                    int id = rst.getInt(1);
                    System.out.println("Done! Id = " + id);
                }
            } else {
                System.out.println("No rows affected!");
            }

        } catch (SQLException | ParseException e) {
            e.printStackTrace();

        } finally {
//            DB.closeStatement(st);
//            DB.closeConnection();
        }

        //atualiza dados no banco
        try {
            connection = DB.getConnection();

            ps = connection.prepareStatement(
                    "UPDATE seller " +
                            "SET BaseSalary = BaseSalary + ?" +
                            "WHERE " +
                            "(DepartmentId = ?)");

            ps.setDouble(1, 200.0);
            ps.setInt(2, 2);

            int rowsAffected = ps.executeUpdate();

            System.out.println("Done! Rows affected: " + rowsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
//            DB.closeStatement(ps);
//            DB.closeConnection();
        }

        //deletar dados do banco
        try {
            connection = DB.getConnection();

            ps = connection.prepareStatement(
                    "DELETE FROM department " +
                            "WHERE " +
                            "Id = ?");

            ps.setInt(1, 2);

            int rowsAffected = ps.executeUpdate();

            System.out.println("Done! Rows affected: " + rowsAffected);
        } catch (SQLException e) {
            throw new DbIntegrityException(e.getMessage());
        } finally {
            DB.closeStatement(ps);
            DB.closeConnection();
        }

        //transações no banco
        try {
            connection = DB.getConnection();

            connection.setAutoCommit(false);

            st = connection.createStatement();

            int rows1 = st.executeUpdate("UPDATE seller " +
                    "SET BaseSalary = 2090 " +
                    "WHERE DepartmentId = 1");

            //provocando uma exceção para testar os comandos setAutoCommit(false) e commit()
//            int x = 1;
//            if (x < 2) {
//                throw new SQLException("Fake error");
//            }

            int rows2 = st.executeUpdate("UPDATE seller " +
                    "SET BaseSalary = 3090 " +
                    "WHERE DepartmentId = 2");

            connection.commit();

            System.out.println("rows1 " + rows1);
            System.out.println("rows2 " + rows2);

        } catch (SQLException e) {
            try {
                connection.rollback();
                throw new DbException("Transaction rolled back! Caused by: " + e.getMessage());
            } catch (SQLException ex) {
                throw new DbException("Error trying to rollback! Caused by: " + ex.getMessage());
            }
        } finally {
            DB.closeStatement(ps);
            DB.closeConnection();
        }
    }

}