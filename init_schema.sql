CREATE TABLE IF NOT EXISTS qttg_master (
    id BIGINT PRIMARY KEY,
    so_so_bhxh VARCHAR(20) UNIQUE NOT NULL,
    nld_id BIGINT,
    thang_bd VARCHAR(10),
    thang_kt VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_master_so_so_bhxh ON qttg_master(so_so_bhxh);

CREATE TABLE IF NOT EXISTS qttg_detail (
    id BIGINT PRIMARY KEY,
    master_id BIGINT NOT NULL,
    nld_id BIGINT,
    tu_thang VARCHAR(10),
    den_thang VARCHAR(10),
    ma_don_vi VARCHAR(50),
    ten_don_vi VARCHAR(255),
    chuc_danh_cv VARCHAR(255),
    muc_luong DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_detail_master FOREIGN KEY (master_id) 
        REFERENCES qttg_master(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_detail_master_id ON qttg_detail(master_id);
CREATE INDEX IF NOT EXISTS idx_detail_ten_don_vi ON qttg_detail(ten_don_vi);