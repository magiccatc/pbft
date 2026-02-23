package main

import (
	"crypto/sha256"
	"log"
	"math/big"
	"os"
	"strconv"
)

func GetLagrange(xs []*big.Int, xpos int, N, dato *big.Int) *big.Int {
	var poly *big.Int = big.NewInt(1)
	denominator := big.NewInt(1)
	for i := 0; i < len(xs); i++ {
		if i != xpos {
			currentTerm := big.NewInt(1).Sub(big.NewInt(0), xs[i])
			//			denominator *= xs[xpos] - xs[i]
			denominator = denominator.Mul(denominator, big.NewInt(0).Sub(xs[xpos], xs[i]))
			poly.Mul(poly, currentTerm)
		}
	}
	poly.Mul(dato, poly)
	poly.Div(poly, denominator)
	return poly
}

func getSi(X *big.Int) (*big.Int, *big.Int, *big.Int) {
	P := big.NewInt(7297)
	Q := big.NewInt(7873)
	e := big.NewInt(7477)
	n := big.NewInt(1)
	n.Mul(P, Q)
	phiN1 := new(big.Int).Sub(P, big.NewInt(1))
	phiN1.Div(phiN1, big.NewInt(2))
	phiN2 := new(big.Int).Sub(Q, big.NewInt(1))
	phiN2.Div(phiN2, big.NewInt(2))
	phiN := big.NewInt(2)
	phiN.Mul(phiN1, phiN2)
	// RSA私钥d
	ie := new(big.Int)
	ie.ModInverse(e, phiN)
	k := threshold
	var randomNumbers []*big.Int
	randomNumbers = append(randomNumbers, ie)
	randomNumbers = append(randomNumbers, big.NewInt(1159))
	randomNumbers = append(randomNumbers, big.NewInt(3471))
	randomNumbers = append(randomNumbers, big.NewInt(2273))
	randomNumbers = append(randomNumbers, big.NewInt(359))
	randomNumbers = append(randomNumbers, big.NewInt(197))
	randomNumbers = append(randomNumbers, big.NewInt(879))
	randomNumbers = append(randomNumbers, big.NewInt(779))
	randomNumbers = append(randomNumbers, big.NewInt(1017))
	randomNumbers = append(randomNumbers, big.NewInt(1358))
	randomNumbers = append(randomNumbers, big.NewInt(196))
	randomNumbers = append(randomNumbers, big.NewInt(2879))
	randomNumbers = append(randomNumbers, big.NewInt(1779))
	randomNumbers = append(randomNumbers, big.NewInt(1053))

	randomNumbers = append(randomNumbers, big.NewInt(113))
	randomNumbers = append(randomNumbers, big.NewInt(23471))
	randomNumbers = append(randomNumbers, big.NewInt(157))
	randomNumbers = append(randomNumbers, big.NewInt(3359))
	randomNumbers = append(randomNumbers, big.NewInt(2197))
	randomNumbers = append(randomNumbers, big.NewInt(3879))
	randomNumbers = append(randomNumbers, big.NewInt(5779))
	randomNumbers = append(randomNumbers, big.NewInt(101))
	randomNumbers = append(randomNumbers, big.NewInt(3356))
	randomNumbers = append(randomNumbers, big.NewInt(275))
	randomNumbers = append(randomNumbers, big.NewInt(3877))
	randomNumbers = append(randomNumbers, big.NewInt(177))
	randomNumbers = append(randomNumbers, big.NewInt(2063))

	randomNumbers = append(randomNumbers, big.NewInt(678))
	randomNumbers = append(randomNumbers, big.NewInt(1321))
	randomNumbers = append(randomNumbers, big.NewInt(3526))
	randomNumbers = append(randomNumbers, big.NewInt(2775))
	randomNumbers = append(randomNumbers, big.NewInt(3657))
	randomNumbers = append(randomNumbers, big.NewInt(1787))
	randomNumbers = append(randomNumbers, big.NewInt(2893))

	randomNumbers = append(randomNumbers, big.NewInt(3221))
	randomNumbers = append(randomNumbers, big.NewInt(37126))
	randomNumbers = append(randomNumbers, big.NewInt(6542))
	randomNumbers = append(randomNumbers, big.NewInt(3482))
	randomNumbers = append(randomNumbers, big.NewInt(5278))

	randomNumbers = append(randomNumbers, big.NewInt(1537))
	randomNumbers = append(randomNumbers, big.NewInt(34718))
	randomNumbers = append(randomNumbers, big.NewInt(21273))
	randomNumbers = append(randomNumbers, big.NewInt(9359))
	randomNumbers = append(randomNumbers, big.NewInt(1997))
	randomNumbers = append(randomNumbers, big.NewInt(33879))
	randomNumbers = append(randomNumbers, big.NewInt(79))
	randomNumbers = append(randomNumbers, big.NewInt(5297))
	randomNumbers = append(randomNumbers, big.NewInt(13598))
	randomNumbers = append(randomNumbers, big.NewInt(1976))
	randomNumbers = append(randomNumbers, big.NewInt(28379))
	randomNumbers = append(randomNumbers, big.NewInt(5320))
	randomNumbers = append(randomNumbers, big.NewInt(523))

	randomNumbers = append(randomNumbers, big.NewInt(9852))
	randomNumbers = append(randomNumbers, big.NewInt(5230))
	randomNumbers = append(randomNumbers, big.NewInt(3752))
	randomNumbers = append(randomNumbers, big.NewInt(6529))
	randomNumbers = append(randomNumbers, big.NewInt(78951))
	randomNumbers = append(randomNumbers, big.NewInt(52301))
	randomNumbers = append(randomNumbers, big.NewInt(7850))
	randomNumbers = append(randomNumbers, big.NewInt(10187))
	randomNumbers = append(randomNumbers, big.NewInt(7853))
	randomNumbers = append(randomNumbers, big.NewInt(9275))
	randomNumbers = append(randomNumbers, big.NewInt(7423))
	randomNumbers = append(randomNumbers, big.NewInt(7177))
	randomNumbers = append(randomNumbers, big.NewInt(89))

	randomNumbers = append(randomNumbers, big.NewInt(88891))
	randomNumbers = append(randomNumbers, big.NewInt(7753))
	randomNumbers = append(randomNumbers, big.NewInt(85230))
	randomNumbers = append(randomNumbers, big.NewInt(4210))
	randomNumbers = append(randomNumbers, big.NewInt(36557))
	randomNumbers = append(randomNumbers, big.NewInt(18787))
	randomNumbers = append(randomNumbers, big.NewInt(2971))

	randomNumbers = append(randomNumbers, big.NewInt(123))
	randomNumbers = append(randomNumbers, big.NewInt(98512))
	randomNumbers = append(randomNumbers, big.NewInt(7478))
	randomNumbers = append(randomNumbers, big.NewInt(6521))
	randomNumbers = append(randomNumbers, big.NewInt(6130))

	randomNumbers = append(randomNumbers, big.NewInt(7413))
	randomNumbers = append(randomNumbers, big.NewInt(9666))
	randomNumbers = append(randomNumbers, big.NewInt(1901))
	randomNumbers = append(randomNumbers, big.NewInt(73356))
	randomNumbers = append(randomNumbers, big.NewInt(98275))
	randomNumbers = append(randomNumbers, big.NewInt(387))
	randomNumbers = append(randomNumbers, big.NewInt(6323))
	randomNumbers = append(randomNumbers, big.NewInt(27763))

	randomNumbers = append(randomNumbers, big.NewInt(98887))
	randomNumbers = append(randomNumbers, big.NewInt(37777))
	randomNumbers = append(randomNumbers, big.NewInt(6743))
	randomNumbers = append(randomNumbers, big.NewInt(87777))
	randomNumbers = append(randomNumbers, big.NewInt(67531))
	randomNumbers = append(randomNumbers, big.NewInt(54447))
	randomNumbers = append(randomNumbers, big.NewInt(79995))

	randomNumbers = append(randomNumbers, big.NewInt(1302))
	randomNumbers = append(randomNumbers, big.NewInt(78923))
	randomNumbers = append(randomNumbers, big.NewInt(75230))
	randomNumbers = append(randomNumbers, big.NewInt(4347))
	randomNumbers = append(randomNumbers, big.NewInt(37773))
	f := ie
	for i := 1; i <= k-1; i++ {
		term := new(big.Int).Exp(X, big.NewInt(int64(i)), phiN)
		term.Mul(randomNumbers[i], term)
		f.Add(f, term)
	}
	return f, phiN, e
}

func (p *hotsutff) getparts(si, dato, mess, m *big.Int) *big.Int {
	mul := big.NewInt(1)
	mul.Mul(si, mul)
	mul.Mul(mul, dato)
	mul.Mul(mul, big.NewInt(2))
	x1 := big.NewInt(1)
	return x1.Exp(mess, mul, m)
}

func Factorial(l int) *big.Int {
	result := big.NewInt(1)
	for i := 2; i <= l; i++ {
		result.Mul(result, big.NewInt(int64(i)))
	}
	return result
}

func computce(dato, N *big.Int) *big.Int {
	ce := big.NewInt(1)
	ce.Mul(dato, dato)
	ce.Mul(big.NewInt(4), ce)
	return ce
}

func dealmessage(hashed []byte, N *big.Int) *big.Int {
	h := sha256.New()
	h.Write(hashed)
	hash := h.Sum(nil)
	m := new(big.Int).SetBytes(hash)
	return m.Mod(m, N)
}

func (p *hotsutff) unionsign(x, xs []*big.Int, e, m, dato *big.Int, k int) *big.Int {
	lamda := getlamda(m, dato, k, xs)
	x[0].Exp(x[0], lamda[0], m)
	x[0].Exp(x[0], e, m)
	for j := 1; j < k; j++ {
		x[j].Exp(x[j], lamda[j], m)
		x[j].Exp(x[j], e, m)
		x[0].Mul(x[0], x[j])
		x[0].Mod(x[0], m)
	}
	return x[0]
}

func getlamda(m, dato *big.Int, k int, xs []*big.Int) []*big.Int {
	var lamda []*big.Int
	for i := 0; i < k; i++ {
		lamdai := GetLagrange(xs, i, m, dato)
		lamdai.Mul(big.NewInt(2), lamdai)
		lamda = append(lamda, lamdai)
	}
	return lamda
}

func verify(dato, m, mess, unionsign *big.Int) bool {
	ce := computce(dato, m)
	ce.Exp(mess, ce, m)
	//fmt.Println(unionsign)
	//fmt.Println(ce)
	//unionsign.Cmp(ce) == 0
	return true
}

// 生成指定数量的RSA私钥
func genRsaKeys(count int) {
	log.Printf("开始生成 %d 个节点的RSA私钥...", count)

	// 创建tKeys目录
	if !isExist("./tKeys") {
		err := os.Mkdir("tKeys", 0644)
		if err != nil {
			log.Fatalf("创建tKeys目录失败: %v", err)
		}
	}

	// 生成每个节点的私钥
	generatedCount := 0
	for i := 0; i < count; i++ {
		// 创建节点目录
		nodeDir := "./tKeys/" + strconv.Itoa(i)
		if !isExist(nodeDir) {
			err := os.Mkdir(nodeDir, 0644)
			if err != nil {
				log.Fatalf("创建节点目录失败: %v", err)
			}
		}

		// 生成私钥
		s, _, _ := getSi(big.NewInt(int64(i + 1)))
		privFileName := "tKeys/" + strconv.Itoa(i) + "/" + strconv.Itoa(i) + "_tRSA_PIV"

		// 如果文件已存在，询问是否覆盖（这里直接覆盖）
		file, err := os.OpenFile(privFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("创建私钥文件失败: %v", err)
		}
		file.Write(s.Bytes())
		file.Close()
		generatedCount++

		if (i+1)%10 == 0 || i == count-1 {
			log.Printf("已生成 %d/%d 个节点的私钥", i+1, count)
		}
	}

	log.Printf("成功生成 %d 个节点的RSA私钥！", generatedCount)
}

// 兼容旧接口（已废弃，保留用于向后兼容）
func tgenRsaKey() {
	genRsaKeys(nodeCount)
}

// 判断文件或文件夹是否存在
func isExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		log.Println(err)
		return false
	}
	return true
}

// 检查所有节点的私钥文件是否存在
func checkAllPrivateKeys() bool {
	missingKeys := []int{}
	for i := 0; i < nodeCount; i++ {
		privFileName := "tKeys/" + strconv.Itoa(i) + "/" + strconv.Itoa(i) + "_tRSA_PIV"
		if !isExist(privFileName) {
			missingKeys = append(missingKeys, i)
		}
	}

	if len(missingKeys) > 0 {
		log.Printf("错误: 检测到缺少 %d 个节点的私钥文件（共需要 %d 个节点）", len(missingKeys), nodeCount)
		log.Printf("缺少的节点ID: %v", missingKeys)
		log.Printf("请运行程序生成所有私钥，或确保 tKeys 目录下存在所有节点的私钥文件")
		return false
	}
	return true
}
