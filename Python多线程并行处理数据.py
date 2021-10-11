import numpy as np
import pandas as pd
import time
from functools import reduce
from multiprocessing.dummy import Pool as ThreadPool

'''
    文本特点：
    以item为起始点，周期性存储
'''


class ParallelProcess:
    '''
        并行处理数据类

    '''

    def __init__(self, path, jobs):
        '''
        构造函数
        :param path: 文件路径
        :param jobs: 并行数
        '''
        self.path = path
        self.jobs = int(jobs)
        # 读取数据
        self._readData()
        # 获取index
        self._getIndex()

    def _readData(self):
        '''
        读取数据，每一行
        :return:
        '''
        with open(self.path, 'r') as f:
            self.fs = f.readlines()

    def _getIndex(self):
        # 识别每个item的index，以此分块
        self.index_list = []
        for index, f in enumerate(self.fs):
            # 根据item标示位，获取index.
            # 如果当前行包含item, 则获取index
            if 'item' in f:
                self.index_list.append(index)

    def _process(self, index):
        '''
        根据index处理对应部分的数据
        :param index:
        :return:
        '''
        df = pd.DataFrame()
        df.loc[1, 'One'] = int(self.fs[index + 1]) + 100
        df.loc[1, 'Two'] = int(self.fs[index + 2]) + 200
        '''
            do something
        '''
        time.sleep(1)
        return df

    def _reduceData(self, df1, df2):
        '''
        合并结果
        :param df1:
        :param df2:
        :return:
        '''
        df1 = pd.concat([df1, df2], ignore_index=True)
        return df1

    def parallel(self):
        '''
        并行处理
        :return: 返回合并好的DataFrame
        '''
        self.pool = ThreadPool(self.jobs)

        # map(functions, [])
        self.results = self.pool.map(self._process, self.index_list)

        # close the pool and wait for the work to finish
        self.pool.close()
        self.pool.join()

        # reduce(functions, [])
        df_final = reduce(self._reduceData, self.results)
        return df_final


if __name__ == '__main__':
    print('Start..')
    start_time = time.time()

    path = r''  # 文件路径
    jobs = 4  # 并行数
    p = ParallelProcess(path, jobs)
    res = p.parallel()

    end_time = time.time()
    cost_time = end_time - start_time
    print('Cost time:{0}'.format(cost_time))
