package io.dbmaster.tools.excelsync;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList
import java.util.Iterator
import java.util.List
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry

import ExcelSync.KeyWrapper;

import com.branegy.service.base.api.ProjectService;
import com.branegy.service.core.QueryRequest
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcConnector
import com.branegy.email.EmailSender

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import java.text.SimpleDateFormat;

class ExcelIterator implements Closeable{
    final File file;
    final List<String> columnList;
    Row row;
    
    public ExcelIterator(File file, List<String> headerColumnList) throws IOException{
        this.file = file;
        InputStream is = null;
        try {
            Workbook workbook;
            if (!file.exists()){
                workbook = new XSSFWorkbook();
                Sheet sheet = workbook.createSheet();
                row = sheet.createRow(0);
                int i=0;
                for (String c:headerColumnList){
                    row.createCell(i++).setCellValue(c);
                }
            } else {
                is = FileUtils.openInputStream(file);
                workbook = new XSSFWorkbook(is);
            }
            
            Sheet sheet = workbook.getSheetAt(0);
            row = sheet.getRow(0);
            List<String> list = new ArrayList<String>();
            for (Cell cell:row){
                String stringCellValue = cell.getStringCellValue();
                if (stringCellValue == null){
                    throw new IllegalStateException("Cell header cell can't be null at index "+cell.getRowIndex()+":"+cell.getColumnIndex());
                }
                list.add(stringCellValue);
            }
            columnList = Collections.unmodifiableList(list);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
   
    public boolean hasMoreRow() {
        return row.getRowNum()<row.getSheet().getLastRowNum();
    }
    public void nextRow() {
        if (!hasMoreRow()){
            row = row.getSheet().createRow(row.getRowNum()+1);
        } else {
            row = row.getSheet().getRow(row.getRowNum()+1);
        }
    }
    public Object getColumn(int index) {
        Cell cell = row.getCell(index);
        return cell == null ? null : cell.getStringCellValue();
    }
    public void setColumn(int index, Object object) {
        Cell cell = row.getCell(index);
        if (cell == null){
            cell = row.createCell(index);
        }
        cell.setCellValue((String)object);
    }
    public List<String> getColumnList() {
        return columnList;
    }
    

    public void close() throws IOException {
        OutputStream os = null;
        try{
            os = FileUtils.openOutputStream(file);
            row.getSheet().getWorkbook().write(os);
        } finally {
            IOUtils.closeQuietly(os);
        }
    }
    
}

public class ExcelSync{
    private final File file;
    
    private final List<String> columnList;
    private final List<String> keyColumnList;
    private final String statusColumn;
    private final String newStatusStatus;
    private final String autoCloseStatus;
    private final Set<String> ignoreAutoCloseStatusSet;
    private final Set<String> nonOpenStatusSet;
    private final Logger logger;
    private final boolean backup;
    
    private final int[] keyIndex;
    private final int[] columnIndex;
    
    private Map<KeyWrapper, Object[]> existsRecords;
    
    static class KeyWrapper{
        final Object[] keys;

        public KeyWrapper(Object[] keys) {
            this.keys = keys;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof KeyWrapper)){
                return false;
            }
            return Arrays.equals(keys, ((KeyWrapper) obj).keys);
        }
    }
    
    public ExcelSync(List<String> columnList, List<String> keyColumnList, String statusColumn,
            String newStatusStatus,
            String autoCloseStatus,
            Set<String> ignoreAutoCloseStatusSet,
            Set<String> nonOpenStatusSet,
            File file,
            Logger logger, boolean backup){
        
        if (!columnList.containsAll(keyColumnList)){
            throw new IllegalArgumentException(""+columnList+" doesn't containt "+keyColumnList);
        }
        if (!columnList.contains(statusColumn)){
            throw new IllegalArgumentException(""+columnList+" doesn't containt "+statusColumn);
        }
        if (keyColumnList.contains(statusColumn)){
            throw new IllegalArgumentException(""+keyColumnList+" containts "+statusColumn);
        }
        
        keyIndex = new int[keyColumnList.size()];
        for (int i=0; i<keyColumnList.size(); ++i){
            String c = keyColumnList.get(i);
            keyIndex[i] = columnList.indexOf(c);
        }
        
        int j = 0;
        columnIndex = new int[columnList.size()-keyColumnList.size()-1];
        for (int i=0; i<columnList.size(); ++i){
            String c = columnList.get(i);
            if (keyColumnList.contains(c) || c.equals(statusColumn)){
                continue;
            }
            columnIndex[j++] = i;
        }
        existsRecords = new LinkedHashMap<KeyWrapper, Object[]>();
        this.file = file;
        this.columnList = columnList;
        this.keyColumnList = keyColumnList;
        this.statusColumn = statusColumn;
        this.newStatusStatus = newStatusStatus;
        this.autoCloseStatus = autoCloseStatus;
        this.ignoreAutoCloseStatusSet = ignoreAutoCloseStatusSet;
        this.nonOpenStatusSet = nonOpenStatusSet;
        this.logger = logger;
        this.backup = backup;
    }
    
    public void addRow(Object... args){
        Object[] keys = new Object[keyIndex.length];
        for (int i=0; i<keyIndex.length; ++i){
            keys[i] = args[keyIndex[i]];
        }
        Object[] values = new Object[columnIndex.length];
        for (int i=0; i<columnIndex.length; ++i){
            values[i] = args[columnIndex[i]];
        }
        if (existsRecords.put(new KeyWrapper(keys),values)!=null) {
            logger.warn("Key already exists {}", Arrays.toString(keys))
        }
    }
    
    public int[] syncAndReturnScore(){
        if (backup && file.exists()){
            FileUtils.copyFile(file, new File(file.getParentFile(), 
                FilenameUtils.getBaseName(file.getName())
                  + "_"+new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
                  + "."+FilenameUtils.getExtension(file.getName())));
        }
        ExcelIterator eit = null;
        try {
            eit = new ExcelIterator(file, columnList);
            if (!eit.getColumnList().containsAll(columnList)){
               throw new IllegalArgumentException(""+eit.getColumnList()+" doesn't containt "+columnList);
            }
            
            // prepare keys
            int[] keyIndexExcel = new int[keyIndex.length];
            for (int i=0; i<keyColumnList.size(); ++i){
                String c = keyColumnList.get(i);
                keyIndexExcel[i] = eit.getColumnList().indexOf(c);
            }
            // prepare status column
            int statusColumnIndexExcel = eit.getColumnList().indexOf(statusColumn);
            // prepare values
            int j = 0;
            int[] columnIndexExcel = new int[columnList.size()-keyColumnList.size()-1];
            for (int i=0; i<columnList.size(); ++i){
                String c = columnList.get(i);
                if (keyColumnList.contains(c) || c.equals(statusColumn)){
                    continue;
                }
                columnIndexExcel[j++] = eit.getColumnList().indexOf(c);
            }
            
            Object[] keys = new Object[keyIndexExcel.length];
            int newScore = 0;
            int autoClosedScore = 0;
            int openScore = 0;
            while (eit.hasMoreRow()){
                eit.nextRow();
                for (int i=0; i<keys.length; ++i){
                    keys[i] = eit.getColumn(keyIndexExcel[i]);
                }
                KeyWrapper key = new KeyWrapper(keys);
                Object[] values = existsRecords.remove(key);
                if (values != null){ // update all columns
                    for (int i=0; i<columnIndexExcel.length; ++i){
                        eit.setColumn(columnIndexExcel[i], values[i]);
                    }
                    if (!ignoreAutoCloseStatusSet.contains(eit.getColumn(statusColumnIndexExcel))){ // force new status
                        if (!newStatusStatus.equals(eit.getColumn(statusColumnIndexExcel))){
                            eit.setColumn(statusColumnIndexExcel, newStatusStatus);
                            newScore++;
                        }
                    }
                } else {
                    if (!ignoreAutoCloseStatusSet.contains(eit.getColumn(statusColumnIndexExcel))){ // force autoclosed status
                        if (!autoCloseStatus.equals(eit.getColumn(statusColumnIndexExcel))){
                            eit.setColumn(statusColumnIndexExcel, autoCloseStatus);
                            autoClosedScore++;
                        }
                    }
                }
                if (!nonOpenStatusSet.contains(eit.getColumn(statusColumnIndexExcel))){ // open status
                    openScore++;
                }
            }
            // new status
            newScore += existsRecords.size();
            openScore += existsRecords.size();
            for (Map.Entry<KeyWrapper, Object[]> e:existsRecords.entrySet()){
                eit.nextRow();
                keys = e.getKey().keys;
                Object[] values = e.getValue();
                for (int i=0; i<keys.length; ++i){
                    eit.setColumn(keyIndexExcel[i], keys[i]);
                }
                for (int i=0; i<columnIndexExcel.length; ++i){
                    eit.setColumn(columnIndexExcel[i], values[i]);
                }
                eit.setColumn(statusColumnIndexExcel, newStatusStatus);
            }
            int[] score = new int[3];
            score[0] = newScore;
            score[1] = autoClosedScore;
            score[2] = openScore;;
            return score;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            IOUtils.closeQuietly(eit);
        }
    }
    
}

