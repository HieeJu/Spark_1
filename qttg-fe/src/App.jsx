import React, { useState, useEffect } from 'react';
import { Table, Input, Card, Typography, Space, Tag, Button, Drawer, Descriptions, Statistic, Row, Col, message } from 'antd';
import { SearchOutlined, EyeOutlined, FileExcelOutlined, ReloadOutlined } from '@ant-design/icons';
import * as XLSX from 'xlsx';
import { saveAs } from 'file-saver';
import { searchQttg } from './services/qttgService';

const { Title, Text } = Typography;
const { Search } = Input;

function App() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [keyword, setKeyword] = useState('');
  const [page, setPage] = useState(0);
  const [size, setSize] = useState(5);
  const [total, setTotal] = useState(0);

  // State quản lý Drawer xem chi tiết
  const [drawerVisible, setDrawerVisible] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState(null);

  // Hàm gọi API lấy dữ liệu
  const fetchData = async (searchKeyword, currentPage, pageSize) => {
    setLoading(true);
    try {
      const response = await searchQttg(searchKeyword, currentPage, pageSize);
      setData(response.data.content);
      setTotal(response.data.totalElements);
    } catch (error) {
      console.error("Lỗi khi kết nối API:", error);
      message.error("Không thể kết nối tới Server Backend!");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData(keyword, page, size);
  }, [keyword, page, size]);

  // Hàm mở Drawer xem chi tiết
  const handleViewDetail = (record) => {
    setSelectedRecord(record);
    setDrawerVisible(true);
  };

  // Hàm Xuất File Excel
  const handleExportExcel = () => {
    if (!data || data.length === 0) {
      message.warning("Không có dữ liệu để xuất Excel!");
      return;
    }

    // Biến đổi dữ liệu sang dạng phẳng để xuất Excel
    const excelData = [];
    data.forEach(master => {
      if (master.details && master.details.length > 0) {
        master.details.forEach(detail => {
          excelData.push({
            "ID Master": master.id,
            "Số Sổ BHXH": master.soSoBhxh,
            "ID Người Lao Động": master.nldId,
            "Từ Tháng": detail.tuThang,
            "Đến Tháng": detail.denThang,
            "Mã Đơn Vị": detail.maDonVi,
            "Tên Đơn Vị": detail.tenDonVi,
            "Chức Danh": detail.chucDanhCv || 'Chưa cập nhật',
            "Mức Lương (VNĐ)": detail.mucLuong ? Number(detail.mucLuong).toLocaleString('vi-VN') : 0
          });
        });
      } else {
        excelData.push({
          "ID Master": master.id,
          "Số Sổ BHXH": master.soSoBhxh,
          "ID Người Lao Động": master.nldId,
          "Từ Tháng": "-",
          "Đến Tháng": "-",
          "Mã Đơn Vị": "-",
          "Tên Đơn Vị": "-",
          "Chức Danh": "-",
          "Mức Lương (VNĐ)": 0
        });
      }
    });

    const worksheet = XLSX.utils.json_to_sheet(excelData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "BaoCaoBHXH");

    const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    const blobData = new Blob([excelBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;charset=UTF-8' });
    saveAs(blobData, `Bao_Cao_BHXH_${new Date().getTime()}.xlsx`);
    message.success("Xuất báo cáo Excel thành công!");
  };

  // Cột Bảng Master
  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 70 },
    { 
      title: 'Số sổ BHXH', 
      dataIndex: 'soSoBhxh', 
      key: 'soSoBhxh',
      render: text => text ? <Tag color="blue">{text}</Tag> : <Tag color="default">N/A</Tag>
    },
    { title: 'ID NLĐ', dataIndex: 'nldId', key: 'nldId' },
    { title: 'Tháng BĐ', dataIndex: 'thangBd', key: 'thangBd', render: text => formatDate(text) || '-' },
    { title: 'Tháng KT', dataIndex: 'thangKt', key: 'thangKt', render: text => formatDate(text) || '-' },
    {
      title: 'Hành động',
      key: 'action',
      render: (_, record) => (
        <Button 
          type="primary" 
          ghost 
          icon={<EyeOutlined />} 
          size="small"
          onClick={() => handleViewDetail(record)}
        >
          Xem chi tiết
        </Button>
      )
    }
  ];

  return (
    <div style={{ padding: '24px', backgroundColor: '#f0f2f5', minHeight: '100vh' }}>
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        
        {/* Header & Thống kê */}
        <Card style={{ borderRadius: '8px' }}>
          <Row justify="space-between" align="middle">
            <Col>
              <Title level={3} style={{ margin: 0, color: '#1890ff' }}>
                HỆ THỐNG QUẢN LÝ QUÁ TRÌNH THAM GIA BHXH
              </Title>
              <Text type="secondary">Tra cứu, quản lý và xuất báo cáo dữ liệu đóng BHXH</Text>
            </Col>
            <Col>
              <Statistic title="Tổng số bản ghi tìm thấy" value={total} suffix="sổ" />
            </Col>
          </Row>
        </Card>

        {/* Thanh công cụ Tìm kiếm & Xuất Excel */}
        <Card style={{ borderRadius: '8px' }}>
          <Row gutter={16} align="middle" justify="space-between">
            <Col span={12}>
              <Search
                placeholder="Nhập số sổ BHXH hoặc tên đơn vị..."
                allowClear
                enterButton={<Button type="primary" icon={<SearchOutlined />}>Tìm kiếm</Button>}
                size="large"
                onSearch={(value) => {
                  setKeyword(value);
                  setPage(0);
                }}
              />
            </Col>
            <Col>
              <Space>
                <Button 
                  icon={<ReloadOutlined />} 
                  size="large" 
                  onClick={() => fetchData(keyword, page, size)}
                >
                  Làm mới
                </Button>
                <Button 
                  type="primary" 
                  danger 
                  icon={<FileExcelOutlined />} 
                  size="large"
                  onClick={handleExportExcel}
                >
                  Xuất Báo Cáo Excel
                </Button>
              </Space>
            </Col>
          </Row>
        </Card>

        {/* Bảng Dữ liệu Chính */}
        <Card style={{ borderRadius: '8px' }}>
          <Table
            columns={columns}
            dataSource={data}
            rowKey="id"
            loading={loading}
            pagination={{
              current: page + 1,
              pageSize: size,
              total: total,
              showSizeChanger: true,
              pageSizeOptions: ['5', '10', '20'],
              onChange: (p, s) => {
                setPage(p - 1);
                setSize(s);
              }
            }}
          />
        </Card>

        {/* Drawer Màn hình Xem Chi Tiết */}
        <Drawer
          title="CHI TIẾT QUÁ TRÌNH THAM GIA BHXH"
          placement="right"
          width={650}
          onClose={() => setDrawerVisible(false)}
          open={drawerVisible}
        >
          {selectedRecord && (
            <Space direction="vertical" style={{ width: '100%' }} size="large">
              <Descriptions title="Thông tin Sổ BHXH" bordered column={2}>
                <Descriptions.Item label="ID Sổ">{selectedRecord.id}</Descriptions.Item>
                <Descriptions.Item label="Số sổ BHXH">
                  <Tag color="blue">{selectedRecord.soSoBhxh}</Tag>
                </Descriptions.Item>
                <Descriptions.Item label="ID Người lao động">{selectedRecord.nldId}</Descriptions.Item>
                <Descriptions.Item label="Thời gian">{formatDate(selectedRecord.thangBd)} - {formatDate(selectedRecord.thangKt)}</Descriptions.Item>
              </Descriptions>

              <Title level={5}>Lịch sử quá trình công tác tại các đơn vị:</Title>
              
              <Table
                dataSource={selectedRecord.details}
                rowKey="id"
                pagination={false}
                columns={[
                  { title: 'Từ - Đến tháng', render: (_, r) => `${r.tuThang || '-'} ➔ ${r.denThang || '-'}` },
                  { title: 'Tên Đơn Vị', dataIndex: 'tenDonVi', key: 'tenDonVi' },
                  { title: 'Chức Danh', dataIndex: 'chucDanhCv', render: t => t || 'Chưa cập nhật' },
                  { 
                    title: 'Mức Lương', 
                    dataIndex: 'mucLuong', 
                    render: t => t ? `${Number(t).toLocaleString('vi-VN')} đ` : 'N/A' 
                  }
                ]}
              />
            </Space>
          )}
        </Drawer>

      </Space>
    </div>
  );
}
function formatDate(val) {
  if (!val) return '';
  const str = String(val).trim();
  if (str.length === 6) {
    const year = str.substring(0, 4);
    const month = str.substring(4, 6);
    return `${month}/${year}`; 
  }
  return val;
}

export default App;