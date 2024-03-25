from types import SimpleNamespace

# 创建命名空间对象
a = SimpleNamespace()

# 设置属性
a.b = SimpleNamespace()
a.b.c = 42

# 访问属性
print(a.b.d)  # 输出: 42
