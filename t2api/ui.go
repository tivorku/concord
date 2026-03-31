package t2api

import "fmt"

func UOMDisplayName(uom string) string {
	switch uom {
	case "gb":
		return "ГБ"
	case "min":
		return "минут(ы)"
	case "sms":
		return "SMS"
	default:
		return uom
	}
}

func SelectSegment(segments []Segment) (Segment, error) {
	if len(segments) == 0 {
		return Segment{}, fmt.Errorf("Нет доступных сегментов")
	}

	segmentsByUOM := make(map[string][]Segment)
	for _, seg := range segments {
		segmentsByUOM[seg.UOM] = append(segmentsByUOM[seg.UOM], seg)
	}

	fmt.Println("\n=== Выберите тип трафика ===")
	var uomList []string
	i := 1
	for uom := range segmentsByUOM {
		count := 0
		for _, s := range segmentsByUOM[uom] {
			count += s.Count
		}
		fmt.Printf("%d. %s [%d лотов]\n", i, UOMDisplayName(uom), count)
		uomList = append(uomList, uom)
		i++
	}

	fmt.Print("> ")
	var choice int
	fmt.Scanln(&choice)
	if choice < 1 || choice > len(uomList) {
		return Segment{}, fmt.Errorf("Неверный выбор типа трафика")
	}
	selectedUOM := uomList[choice-1]

	selectedSegments := segmentsByUOM[selectedUOM]
	fmt.Printf("\n=== Сегменты %s ===\n", UOMDisplayName(selectedUOM))
	for j, seg := range selectedSegments {
		fmt.Printf("%d. %d %s за %d руб (%d лотов)\n", j+1, seg.Volume, UOMDisplayName(seg.UOM), seg.Cost, seg.Count)
	}

	fmt.Print("> ")
	fmt.Scanln(&choice)
	if choice < 1 || choice > len(selectedSegments) {
		return Segment{}, fmt.Errorf("Неверный выбор сегмента")
	}

	selectedSeg := selectedSegments[choice-1]
	fmt.Printf("[MDN] Выбран сегмент: %d %s за %d руб (%d лотов)\n", selectedSeg.Volume, selectedSeg.UOM, selectedSeg.Cost, selectedSeg.Count)

	return selectedSeg, nil
}
