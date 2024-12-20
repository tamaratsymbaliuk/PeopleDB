package com.tsymbalt.peopledb.repository;

import com.tsymbalt.peopledb.annotation.Id;
import com.tsymbalt.peopledb.annotation.MultiSQL;
import com.tsymbalt.peopledb.annotation.SQL;
import com.tsymbalt.peopledb.exception.UnableToSaveException;
import com.tsymbalt.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    private String getSQLByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        Stream<SQL> multiSQLStream =  Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m-> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .flatMap(msql-> Arrays.stream(msql.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m-> m.isAnnotationPresent(SQL.class))
                .map(m-> m.getAnnotation(SQL.class));

        return Stream.concat(multiSQLStream, sqlStream)
                .filter(a-> a.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.SAVE, this::getSaveSQL), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
                //entity.setId(id);
                setIdByAnnotation(id, entity);
                System.out.println(entity);
            }
            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save a person: " + entity);
        }
        return entity;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteSQL));
            //ps.setLong(1, entity.getId());
            ps.setLong(1, getIdByAnnotation(entity));
            int affectedRecordCount = ps.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Long getIdByAnnotation(T entity) {
       return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f-> f.isAnnotationPresent(Id.class))
                .map(f-> {
                    f.setAccessible(true); // setting this as Id is private so we can access it
                    Long id = null;
                    try {
                        id = (long)f.get(entity);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated field found"));
    }
    private void setIdByAnnotation(Long id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true); // setting this as Id is private so we can access it
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set Id field value");
                    }
                });
    }

    public void delete(T...entities) {
        try {
            Statement stmt = connection.createStatement();
            String ids = Arrays.stream(entities).map(e-> getIdByAnnotation(e)).map(String::valueOf).collect(joining(","));
            int affectedRecordsCount = stmt.executeUpdate(getSQLByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSQL).replace(":ids",ids));
            System.out.println(affectedRecordsCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.UPDATE, this::getUpdateSQL));
            mapForUpdate(entity, ps);
            ps.setLong(5, getIdByAnnotation(entity));
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * @return Should return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */


    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;
    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;


    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSQL));
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSQL));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return entities;
    }
    public long count() {
        long count = 0;
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation(CrudOperation.COUNT, this:: getCountSQL));
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }
    protected String getSaveSQL(){throw new RuntimeException("SQL not defined.");}
    protected String getUpdateSQL() {throw new RuntimeException("SQL not defined.");}
    protected String getDeleteInSQL() {throw new RuntimeException("SQL not defined.");}

    protected String getDeleteSQL(){throw new RuntimeException("SQL not defined.");}
    protected String getCountSQL(){throw new RuntimeException("SQL not defined.");}

    protected String getFindAllSQL(){throw new RuntimeException("SQL not defined.");}

    /**
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected String getFindByIdSQL(){return " ";}
    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;
}
