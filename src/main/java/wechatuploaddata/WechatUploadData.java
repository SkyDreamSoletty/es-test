package wechatuploaddata;

import java.util.Date;

public class WechatUploadData {
    private Integer id;

    private Date uploadDate;

    private Integer dataType;

    private Date date;

    private String md5;

    private Integer exposure;

    private Integer clickAmount;

    private Integer acv;

    private Integer sfv;

    private String imgUrl;

    private String fileName;

    private Double clickExposure;

    private Double acvExposure;

    private Double sfvExposure;

    private Double acvClick;

    private Double sfvClick;

    public WechatUploadData(){}

    public WechatUploadData(Integer id, Date uploadDate, Integer dataType, Date date, String md5, Integer exposure, Integer clickAmount, Integer acv, Integer sfv, String imgUrl, String fileName, Double clickExposure, Double acvExposure, Double sfvExposure, Double acvClick, Double sfvClick) {
        this.id = id;
        this.uploadDate = uploadDate;
        this.dataType = dataType;
        this.date = date;
        this.md5 = md5;
        this.exposure = exposure;
        this.clickAmount = clickAmount;
        this.acv = acv;
        this.sfv = sfv;
        this.imgUrl = imgUrl;
        this.fileName = fileName;
        this.clickExposure = clickExposure;
        this.acvExposure = acvExposure;
        this.sfvExposure = sfvExposure;
        this.acvClick = acvClick;
        this.sfvClick = sfvClick;
    }


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Date getUploadDate() {
        return uploadDate;
    }

    public void setUploadDate(Date uploadDate) {
        this.uploadDate = uploadDate;
    }

    public Integer getDataType() {
        return dataType;
    }

    public void setDataType(Integer dataType) {
        this.dataType = dataType;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public Integer getExposure() {
        return exposure;
    }

    public void setExposure(Integer exposure) {
        this.exposure = exposure;
    }

    public Integer getClickAmount() {
        return clickAmount;
    }

    public void setClickAmount(Integer clickAmount) {
        this.clickAmount = clickAmount;
    }

    public Integer getAcv() {
        return acv;
    }

    public void setAcv(Integer acv) {
        this.acv = acv;
    }

    public Integer getSfv() {
        return sfv;
    }

    public void setSfv(Integer sfv) {
        this.sfv = sfv;
    }

    public String getImgUrl() {
        return imgUrl;
    }

    public void setImgUrl(String imgUrl) {
        this.imgUrl = imgUrl;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Double getClickExposure() {
        return clickExposure;
    }

    public void setClickExposure(Double clickExposure) {
        this.clickExposure = clickExposure;
    }

    public Double getAcvExposure() {
        return acvExposure;
    }

    public void setAcvExposure(Double acvExposure) {
        this.acvExposure = acvExposure;
    }

    public Double getSfvExposure() {
        return sfvExposure;
    }

    public void setSfvExposure(Double sfvExposure) {
        this.sfvExposure = sfvExposure;
    }

    public Double getAcvClick() {
        return acvClick;
    }

    public void setAcvClick(Double acvClick) {
        this.acvClick = acvClick;
    }

    public Double getSfvClick() {
        return sfvClick;
    }

    public void setSfvClick(Double sfvClick) {
        this.sfvClick = sfvClick;
    }
}