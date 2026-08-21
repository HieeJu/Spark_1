import axios from 'axios';

const API_BASE_URL = 'http://localhost:8080/api/v1/qttg';

export const searchQttg = (keyword = '', page = 0, size = 5) => {
    return axios.get(`${API_BASE_URL}/search`, {
        params: { keyword, page, size }
    });
};