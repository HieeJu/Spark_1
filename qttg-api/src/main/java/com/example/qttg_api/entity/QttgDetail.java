package com.example.qttg_api.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "qttg_detail")
@Getter 
@Setter 
@NoArgsConstructor 
@AllArgsConstructor
public class QttgDetail {

    @Id
    @Column(name = "id")
    private Long id;

    // Khai báo quan hệ N - 1 ngược về Master thông qua cột master_id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "master_id", nullable = false)
   @JsonIgnore
    private QttgMaster master;

    @Column(name = "nld_id")
    private Long nldId;

    @Column(name = "tu_thang")
    private String tuThang;

    @Column(name = "den_thang")
    private String denThang;

    @Column(name = "ma_don_vi")
    private String maDonVi;

    @Column(name = "ten_don_vi")
    private String tenDonVi;

    @Column(name = "chuc_danh_cv")
    private String chucDanhCv;

    @Column(name = "muc_luong")
    private Double mucLuong;
}
