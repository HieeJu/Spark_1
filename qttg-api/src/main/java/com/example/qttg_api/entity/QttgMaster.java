package com.example.qttg_api.entity;

import jakarta.persistence.*;
import lombok.*;
import java.util.List;

@Entity
@Table(name = "qttg_master")
@Getter 
@Setter 
@NoArgsConstructor 
@AllArgsConstructor
public class QttgMaster {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "so_so_bhxh", unique = true, nullable = false)
    private String soSoBhxh;

    @Column(name = "nld_id")
    private Long nldId;

    @Column(name = "thang_bd")
    private String thangBd;

    @Column(name = "thang_kt")
    private String thangKt;

    // Khai báo quan hệ 1 - N với bảng Detail
    @OneToMany(mappedBy = "master", fetch = FetchType.LAZY)
    private List<QttgDetail> details;
}
    

