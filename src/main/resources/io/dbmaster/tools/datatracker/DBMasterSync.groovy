package io.dbmaster.tools.datatracker;

import java.io.Closeable
import java.io.File
import java.io.IOException
import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map
import java.util.Set
import java.util.HashSet
import java.util.Map.Entry

import com.branegy.service.base.api.ProjectService
import com.branegy.service.core.QueryRequest
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcConnector

import java.io.File
import java.io.IOException
import java.util.Arrays
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Set
import java.util.LinkedHashMap

import org.apache.commons.io.IOUtils
import org.slf4j.Logger
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import com.branegy.scripting.DbMaster
import com.branegy.dbmaster.custom.CustomFieldConfig
import com.branegy.dbmaster.custom.CustomObjectEntity
import com.branegy.dbmaster.custom.CustomObjectService
import com.branegy.dbmaster.custom.CustomObjectTypeEntity
import com.branegy.dbmaster.custom.field.server.api.ICustomFieldService
import java.text.SimpleDateFormat

public class DBMasterSync {
    private final List<CustomFieldConfig> fields
    private final List<String> columnList
    private final List<String> keyColumnList
    private final String statusColumn
    private final String newStatusStatus
    private final String autoCloseStatus
    private final Set<String> ignoreAutoCloseStatusSet
    private final Set<String> nonOpenStatusSet
    private final Set<String> doNotUpdateFields
    
    private final Logger logger
    private final DbMaster dbm
    
    private final int[] keyIndex
    
    private Map<KeyWrapper, CustomObjectEntity> dbRecords
    private final String storageType
    private final CustomObjectService customService
    private final ICustomFieldService cfService
    private Set<KeyWrapper> updatedKeys
    
    private static class KeyWrapper {
        final Object[] keyValues

        public KeyWrapper(Object[] keyValues) {
            this.keyValues = keyValues
        }
        
        @Override
        public int hashCode() {
            return Arrays.hashCode(keyValues)
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof KeyWrapper)) {
                return false
            }
            return Arrays.equals(keyValues, ((KeyWrapper) obj).keyValues)
        }
    }
    
    public DBMasterSync(
            List<CustomFieldConfig> fields, List<String> keyColumnList, Set<String> doNotUpdateFields,
            String statusColumn, String newStatusStatus, String autoCloseStatus,
            Set<String> ignoreAutoCloseStatusSet, Set<String> nonOpenStatusSet,
            String storageType,
            DbMaster dbm, Logger logger)
    {
        this.customService = dbm.getService(CustomObjectService.class)
        this.cfService = dbm.getService(ICustomFieldService.class)
        this.fields = fields
        this.columnList = fields.collect { it.name }

        keyIndex = new int[keyColumnList.size()]
        for (int i=0; i<keyColumnList.size(); ++i) {
            String c = keyColumnList.get(i)
            keyIndex[i] = columnList.indexOf(c)
        }
        
        this.storageType = storageType
        this.keyColumnList = keyColumnList
        this.statusColumn = statusColumn
        this.newStatusStatus = newStatusStatus
        this.autoCloseStatus = autoCloseStatus
        this.ignoreAutoCloseStatusSet = ignoreAutoCloseStatusSet
        this.nonOpenStatusSet = nonOpenStatusSet
        this.doNotUpdateFields = doNotUpdateFields

        this.dbm = dbm
        this.logger = logger
        
        checkMetadata()
    }
    
    private void checkMetadata() {
        if (!columnList.containsAll(keyColumnList)) {
            throw new IllegalArgumentException("${columnList} doesn't contain ${keyColumnList}")
        }
        if (!columnList.contains(statusColumn)) {
            throw new IllegalArgumentException("${columnList} doesn't contain ${statusColumn}");
        }
        if (keyColumnList.contains(statusColumn)) {
            throw new IllegalArgumentException("${keyColumnList} contains ${statusColumn}");
        }
        
        List<CustomObjectTypeEntity> types = customService.getCustomObjectTypeList()
        def trackingType = types.find { it.getObjectName().equals(storageType)}
        
        if (trackingType==null) {
            trackingType = new CustomObjectTypeEntity()
            trackingType.setObjectName(storageType)
            trackingType.setCreate(false)
            trackingType.setUpdate(true)
            trackingType.setDelete(true)
            trackingType = customService.createCustomObjectType(trackingType)
            
            fields.each { field ->
                cfService.createCustomFieldConfig(trackingType, field, false)
            }
        }
    }
    
    public void loadRecords(String objectFilter) {
        dbRecords = new LinkedHashMap<KeyWrapper, CustomObjectEntity>()
        
        def request = new QueryRequest(objectFilter)
        customService.getCustomObjectSlice(storageType, request).each { row ->
            Object[] keyValues = new Object[keyColumnList.size()]
            for (int i=0; i<keyColumnList.size(); ++i) {
                keyValues[i] = row.getCustomData(keyColumnList.get(i))
            }
            logger.debug("key=${Arrays.toString(keyValues)}")
            if (dbRecords.put(new KeyWrapper(keyValues), row)!=null) {
                logger.warn("Object with key {} already exists", Arrays.toString(keyValues))
            }
        }
        logger.info("Loaded ${dbRecords.size()} records with filter ${objectFilter}")
        updatedKeys = new HashSet<KeyWrapper>()
    }
    
    public CustomObjectEntity checkRecord(Object... args) {
        Object[] keyValues = new Object[keyIndex.length]
        for (int i=0; i<keyIndex.length; ++i) {
            keyValues[i] = args[keyIndex[i]]
        }
        def key = new KeyWrapper(keyValues)
        logger.debug("key2=${Arrays.toString(keyValues)}")
        if (updatedKeys.contains(key)) {
            logger.warn("Object with key {} already exists", Arrays.toString(keyValues))
        }
        CustomObjectEntity record = dbRecords[key]
        if (record==null) {
            record = new CustomObjectEntity()
            record.setDiscriminator(storageType)
            for (int i=0; i<fields.size; i++) {
                record.setCustomData(fields.get(i).name, args[i])
            }
            record.setCustomData(statusColumn, newStatusStatus)
            dbRecords[key] = record
        } else {
            for (int i=0; i<fields.size; i++) {
                def fieldName = fields.get(i).name 
                if (!keyColumnList.contains(fieldName) && !doNotUpdateFields.contains(fieldName)) {
                    record.setCustomData(fieldName, args[i])
                }
            }
            def recordStatus = object.getCustomData(statusColumn)

            // force open status
            if (!ignoreAutoCloseStatusSet.contains(recordStatus)) {
                if (!newStatusStatus.equals(recordStatus)) {
                    recordStatus = newStatusStatus
                    object.setCustomData(statusColumn, newStatusStatus)
                    // newRecords++
                }
            }
        }
        updatedKeys.add(key)
        return record
    }

    public int[] completeSync() {
        try {
            int newRecords = 0
            int autoClosedRecords = 0
            int openRecords = 0
            String recordStatus;
            
            // prepare values
            dbRecords.each { key, object -> 
                if (updatedKeys.contains(key)) {
                    logger.debug("id=${object.getId()} isPersisted=${object.isPersisted()} updatedby=${object.getUpdateAuthor()}")
                    if (object.getId() == 0) {
                        newRecords++;
                        object.setCustomData(statusColumn, newStatusStatus)
                        recordStatus = newStatusStatus
                        customService.createCustomObject(object)
                    } else {
                        customService.updateCustomObject(object)
                        recordStatus = object.getCustomData(statusColumn)
                    }
                } else {
                    // force autoclosed status
                    recordStatus = object.getCustomData(statusColumn)
                    if (!ignoreAutoCloseStatusSet.contains(recordStatus)) {
                        if (!autoCloseStatus.equals(recordStatus)) {
                            object.setCustomData(statusColumn, autoCloseStatus)
                            recordStatus = autoCloseStatus
                            customService.updateCustomObject(object)
                            autoClosedRecords++
                        }
                    }
                }
                // open status
                if (!nonOpenStatusSet.contains(recordStatus)) {
                    openRecords++
                }
            }
            return [ newRecords, autoClosedRecords, openRecords ] as int[]
        } catch (IOException e) {
            throw new IllegalStateException(e)
        }
    }   
}
