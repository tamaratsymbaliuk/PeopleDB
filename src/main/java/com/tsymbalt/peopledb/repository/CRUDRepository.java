package com.tsymbalt.peopledb.repository;

import com.tsymbalt.peopledb.annotation.SQL;
import com.tsymbalt.peopledb.exception.UnableToSaveException;
import com.tsymbalt.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    private String getSQLByAnnotation(String methodName, Supplier<String> sqlGetter) {
        Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m-> methodName.equals(m.getName()))
                .map(m-> m.getAnnotation(SQL.class))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);

    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation("mapForSave", this::getSaveSQL), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
                entity.setId(id);
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
            PreparedStatement ps = connection.prepareStatement(getDeleteSQL());
            ps.setLong(1, entity.getId());
            int affectedRecordCount = ps.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void delete(T...entities) {
        try {
            Statement stmt = connection.createStatement();
            String ids = Arrays.stream(entities).map(T::getId).map(String::valueOf).collect(joining(","));
            int affectedRecordsCount = stmt.executeUpdate(getDeleteInSQL().replace(":ids",ids));
            System.out.println(affectedRecordsCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSQLByAnnotation("mapForUpdate", this::getUpdateSQL));
            mapForUpdate(entity, ps);
            ps.setLong(5, entity.getId());
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected String getUpdateSQL() {return "";}


    /**
     *
     * @return Should return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */
    protected abstract String getDeleteInSQL();

    protected abstract String getDeleteSQL();

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;
    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    String getSaveSQL(){return "";}

    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getFindByIdSQL());
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
            PreparedStatement ps = connection.prepareStatement(getFindAllSQL());
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
            PreparedStatement ps = connection.prepareStatement(getCountSQL());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    protected abstract String getCountSQL();

    protected abstract String getFindAllSQL();

    /**
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected abstract String getFindByIdSQL();
    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;
}
