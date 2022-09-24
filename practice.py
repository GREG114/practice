from pydoc import importfile
from greg_pkg_release.elkhelper import elkhelper
import random

elk = elkhelper("http://121.36.91.101:19200",("testman","testman"))


ln=[
"赵","钱","孙","李",
"冯","陈","褚","卫",
"朱","秦","尤","许",
"孔","曹","严","华",
"戚","谢","邹","喻",
"云","苏","潘","葛",
"鲁","韦","昌","马",
"俞","任","袁","柳"
]

fn=[
    "木玮","超润","卓昊","梓骆","记铭","林木","月玮","远宏","伯峻","伦强","谷韶","彬帆","玮云","天霖","斯辰","寒月","璟良","志嘉","天瑞"
]
age =range(16,19)

bn = "A0201"
total=100
nameused=[]
students=[]
for i in range(1,50):
    name =f"{random.choice(ln)}{random.choice(fn)}"
    while name in nameused:
        name =f"{random.choice(ln)}{random.choice(fn)}"
    obj={
        "id":bn+str(i).zfill(3),
        "age":random.choice(age),
        "name":name
    }
    students.append(obj)

res = elk.bulk("test_students",students)

print(res)